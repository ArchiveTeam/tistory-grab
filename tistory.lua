local urlparse = require("socket.url")
local http = require("socket.http")
local https = require("ssl.https")
local cjson = require("cjson")
local utf8 = require("utf8")
local html_entities = require("htmlEntities")

local item_dir = os.getenv("item_dir")
local warc_file_base = os.getenv("warc_file_base")
local concurrency = tonumber(os.getenv("concurrency"))
local item_type = nil
local item_name = nil
local item_value = nil
local item_user = nil

local url_count = 0
local tries = 0
local downloaded = {}
local seen_200 = {}
local addedtolist = {}
local abortgrab = false
local killgrab = false
local logged_response = false

local discovered_outlinks = {}
local discovered_items = {}
local discovered_extra_items = {}
local bad_items = {}
local ids = {}
local context = {}

local retry_url = false
local is_initial_url = true

local sites = {}
for _, site in pairs(cjson.decode(os.getenv("sites"))) do
  sites[site] = true
end
local assets = cjson.decode(os.getenv("assets"))

site_included = function(domain)
  if sites[domain] then
    return true
  end
  local temp = string.match(domain, "^www%.(.+)$")
  if temp then
    domain = temp
  end
  return sites[domain]
end

get_domain_item = function(domain)
  local temp = string.match(domain, "^https?://(.+)$")
  if temp then
    domain = temp
  end
  domain = string.match(domain, "^([^/]+)")
  if not domain then
    return nil
  end
  local temp = string.match(domain, "^([^%.]+)%.tistory%.com$")
  if temp then
    return temp
  end
  if not site_included(domain) then
    return nil
  end
  return domain
end

abort_item = function(item)
  abortgrab = true
  --killgrab = true
  if not item then
    item = item_name
  end
  if not bad_items[item] then
    io.stdout:write("Aborting item " .. item .. ".\n")
    io.stdout:flush()
    bad_items[item] = true
  end
end

kill_grab = function(item)
  io.stdout:write("Aborting crawling.\n")
  killgrab = true
end

read_file = function(file)
  if file then
    local f = assert(io.open(file))
    local data = f:read("*all")
    f:close()
    return data
  else
    return ""
  end
end

processed = function(url)
  if downloaded[url] or addedtolist[url] then
    return true
  end
  return false
end

discover_item = function(target, item)
  if not target[item] then
--print("discovered", item)
    target[item] = true
    return true
  end
  return false
end

item_patterns = {
  ["^https?://([^/]+)/$"]={
    ["type"]="blog",
    ["additional"]=function(s)
      if s == "www.tistory.com" then
        return nil
      end
      local domain_item = get_domain_item(s)
      if item_type == "blog"
        and domain_item == context["custom_domain"] then
        return nil
      end
      if domain_item then
        return {
          ["value"]=domain_item,
          ["site"]=domain_item
        }
      end
    end
  },
  ["^https?://([^%.]+%.daumcdn%.net/.+)$"]={
    ["type"]="asset",
    ["additional"]=function(s)
      local temp = string.match(s, "^[^/]+/tistory/([0-9]+)/skin/")
      if temp and ids[temp] then
        return nil
      end
      local a, b = string.match(s, "^([a-z]+)[0-9]*(%.daumcdn%.net/.+)")
      if a == "t" or a == "tistory" or a == "img" or a == "i" then
        s = a .. "1" .. b
      end
      if string.match(s, "%?original$") then
        s = string.match(s, "^([^%?]+)%?")
      end
      return {["value"]=s}
    end
  },
  ["^https?://([^%.]+%.kakaocdn%.net/.+)$"]="asset",
  ["^https?://([^/]+/+[a-zA-Z0-9%-_]+)$"]={
    ["type"]="path2",
    ["additional"]=function(s)
      local site, path = string.match(s, "^([^/]+)/+(.+)$")
      site = get_domain_item(s)
      if not site or site == "www" then
        return nil
      end
      if string.match(site, "%.") then
        if context["custom_domain"] and site == context["custom_domain"] then
          site = context["site"]
        elseif context["site"] and site == context["site"] then
          if context["tistory_blog"] then
            site = context["tistory_blog"]
          end
        elseif context["site"] then
          return nil
        end
      end
      local match = string.match(path, "^m/(.*)$")
      if match then
        path = match
        if not string.match(path, "^[a-zA-Z0-9%-_]+$")
          and not string.match(path, "^[a-z]+/[^%?&]+$") then
          return nil
        end
      end
      if string.match(path, "^api/") then
        return nil
      end
      if string.match(path, "archive/[0-9][0-9][0-9][0-9][0-9][0-9]$")
        and context["archive_id"] then
        return nil
      end
      if string.match(path, "^archive/")
        and not string.match(path, "archive/[0-9][0-9][0-9][0-9][0-9][0-9]$") then
        return nil
      end
      result = {
        ["value"]=site .. ":" .. path,
        ["path"]=path,
        ["site"]=site,
        ["extra_ids"]={string.lower(string.match(path, "([^%?&]+)$"))}
      }
      if string.match(path, "^[0-9]+$") then
        result["post_id"] = result["extra_ids"][1]
      elseif string.match(path, "^archive/[0-9]+$") then
        result["archive_id"] = result["extra_ids"][1]
      end
      return result
    end
  },
  ["^https?://([^/]+/+[a-z]+/[^%?&]+)$"]={
    ["type"]="path2",
    ["additional"]="^https?://([^/]+/+[a-zA-Z0-9%-_]+)$"
  },
  ["^https?://tv%.kakao%.com/v/([0-9]+)"]="video",
  ["^https?://play%-tv%.kakao%.com/embed/player/cliplink/([0-9]+)"]="video",
}
for pattern, data in pairs(item_patterns) do
  if type(data) == "string" then
    data = {["type"]=data}
  end
  if not data["additional"] then
    data["additional"] = function(s) return {["value"]=s} end
  end
  if type(data["additional"]) == "string" then
    data["additional"] = item_patterns[data["additional"]]["additional"]
    if not data["additional"] then
      error("Could not initialize item patterns.")
    end
  end
  item_patterns[pattern] = data
end

extraction_patterns = {
  ["^https?://([^/]+)/"]=item_patterns["^https?://([^/]+)/$"],
  ["^https?://([^/]+)"]={
    ["type"]="maybeblog",
    ["additional"]=function(s)
      if string.match(s, "^[^/%.]+%.tistory%.com$")
        or s == context["blog"]
        or not string.match(s, "%.") then
        return nil
      end
      return {["value"]=s}
    end
  },
}
for k, v in pairs(item_patterns) do
  extraction_patterns[k] = v
end
for _, pattern in pairs({
  "^https?://([^/]+)/$",
}) do
  if not extraction_patterns[pattern] then
    error("Could not find pattern.")
  end
  extraction_patterns[pattern] = nil
end

get_item_data = function(url, pattern, pattern_data)
  local value = string.match(url, pattern)
  if not value then
    return nil
  end
  local data = pattern_data["additional"](value)
  if data then
    if not data["type"] then
      data["type"] = pattern_data["type"]
    end
    return data
  end
end

find_item = function(url)
  if ids[url] then
    return nil
  end
  for pattern, data in pairs(item_patterns) do
    local data = get_item_data(url, pattern, data)
    if data then
      return data
    end
  end
end

set_item = function(url)
  found = find_item(url)
  if found then
    local newcontext = {["outlinks"]={}, ["max_post_id"]=0}
    if (found["type"] == "asset" or found["type"] == "asset2")
      and assets[url] then
      found["type"] = assets[url]
    end
    new_item_type = found["type"]
    new_item_value = found["value"]
    for k, v in pairs(found) do
      if k ~= "type" and k ~= "value" then
        newcontext[k] = v
      end
    end
    new_item_name = new_item_type .. ":" .. new_item_value
    if new_item_name ~= item_name
      and (
        not found["image_id"]
        or found["image_id"] ~= context["image_id"]
      ) then
      ids = {}
      context = newcontext
      item_value = new_item_value
      item_type = new_item_type
      ids[string.lower(item_value)] = true
      if context["extra_ids"] then
        for _, extra_id in pairs(context["extra_ids"]) do
          ids[extra_id] = true
        end
      end
      abortgrab = false
      tries = 0
      retry_url = false
      is_initial_url = true
      item_name = new_item_name
      print("Archiving item " .. item_name)
    end
  end
end

percent_encode_url = function(url)
  temp = ""
  for c in string.gmatch(url, "(.)") do
    local b = string.byte(c)
    if b < 32 or b > 126 then
      c = string.format("%%%02X", b)
    end
    temp = temp .. c
  end
  return temp
end

allowed = function(url, parenturl)
  local noscheme = string.match(url, "^https?://(.*)$")

  if ids[url]
    or (noscheme and ids[string.lower(noscheme)]) then
    return true
  end

  if string.match(url, "'%s*%+%s*'")
    or string.match(url, "/%*")
    or string.match(url, "/auth/login/%?")
    or string.match(url, "{[^}]+}")
    or string.match(url, "/comment/add/[0-9]+$")
    or string.match(url, "^https?://[^/]+/m/tag/")
    or (
      string.match(url, "^https?://[^/]+/m/api/")
      and not string.match(url, "^https?://[^/]*tistory%.com/")
    ) then
    return false
  end

  local skip = false
  for pattern, data in pairs(extraction_patterns) do
    match = get_item_data(url, pattern, data)
    if match then
      local new_item = match["type"] .. ":" .. match["value"]
      local to_skip = match["type"] ~= "blog" and match["type"] ~= "maybeblog"
      if new_item ~= item_name then
        if match["type"] == "path2" then
          local dir = string.match(url, "^(https?://[^/]+/.+/).")
          if dir then
            allowed(dir, parenturl)
          end
        end
        discover_item(discovered_items, new_item)
      elseif to_skip then
        return true
      end
      if to_skip then
        skip = true
      end
    end
  end
  if skip then
    return false
  end

  local item_domain = get_domain_item(url)
  local is_cdn = string.match(url, "^https?://[^/]*daumcdn%.net/")
    or string.match(url, "^https?://[^/]*kakao%.com/")
    or string.match(url, "^https?://[^/]*kakaocdn%.net/")

  if not item_domain
    and not is_cdn then
    if not string.match(url, "%.") then
      return false
    end
    if not context["found_domains"] then
      if not context["outlinks"][url] then
        context["outlinks"][url] = {}
      end
      local parent_temp = parenturl
      if not parent_temp then
        parent_temp = "no_parent"
      end
      context["outlinks"][url][parent_temp] = true
    end
    discover_item(discovered_outlinks, string.match(percent_encode_url(url), "^([^%s]+)"))
    return false
  else
    if not string.match(url, "^https?://[^/]+/") then
      url = url .. "/"
    end
    local path = string.match(url, "^https?://[^/]+/([^%?]*)")
    if string.match(path, "^m/") then
      path = string.match(path, "^m/(.*)$")
    end
    if path and ids[string.lower(path)] then
      return true
    end
    if item_type == "blog"
      and (
        (context["custom_domain"] and item_domain == context["custom_domain"])
        or (context["site"] and item_domain == context["site"])
      ) then
      return true
    end
    for _, pattern in pairs({
      "([0-9a-zA-Z_]+)",
      "([^/%?&]+)",
      "([^/]+)"
    }) do
      for s in string.gmatch(url, pattern) do
        if ids[string.lower(s)] then
          return true
        end
      end
    end
  end

  return false
end

wget.callbacks.download_child_p = function(urlpos, parent, depth, start_url_parsed, iri, verdict, reason)
  local url = urlpos["url"]["url"]
  local html = urlpos["link_expect_html"]

  if context["is_new"] then
    return false
  end

  if allowed(url, parent["url"])
    and not processed(url)
    and not addedtolist[url] then
    addedtolist[url] = true
    return true
  end

  return false
end

decode_codepoint = function(newurl)
  newurl = string.gsub(
    newurl, "\\[uU]([0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F])",
    function (s)
      return utf8.char(tonumber(s, 16))
    end
  )
  return newurl
end

percent_encode_url = function(newurl)
  result = string.gsub(
    newurl, "(.)",
    function (s)
      local b = string.byte(s)
      if b < 32 or b > 126 then
        return string.format("%%%02X", b)
      end
      return s
    end
  )
  return result
end

wget.callbacks.get_urls = function(file, url, is_css, iri)
  local urls = {}
  local html = nil
  local json = nil

  downloaded[url] = true

  if context["is_new"] then
    return {}
  end

  if abortgrab then
    return {}
  end

  local function fix_case(newurl)
    if not newurl then
      newurl = ""
    end
    if not string.match(newurl, "^https?://[^/]") then
      return newurl
    end
    if string.match(newurl, "^https?://[^/]+$") then
      newurl = newurl .. "/"
    end
    local a, b = string.match(newurl, "^(https?://[^/]+/)(.*)$")
    return string.lower(a) .. b
  end

  local function check(newurl)
    if not string.match(newurl, "^https?://") then
      return nil
    end
    if string.match(newurl, "[\r\n]") then
      for new in string.gmatch(newurl, "([^\r\n]+)") do
        check(new)
      end
      return nil
    end
    local post_body = nil
    local post_url = nil
    if not newurl then
      newurl = ""
    end
    newurl = decode_codepoint(newurl)
    newurl = fix_case(newurl)
    local origurl = url
    if string.len(url) == 0
      or string.len(newurl) == 0 then
      return nil
    end
    local url = string.match(newurl, "^([^#]+)")
    local url_ = string.match(url, "^(.-)[%.\\]*$")
    while string.find(url_, "&amp;") do
      url_ = string.gsub(url_, "&amp;", "&")
    end
    if string.match(url_, ",") then
      local base = string.match(url_, "^([^,]+)")
      for s in string.gmatch(url_, "([^,]+)") do
        check(urlparse.absolute(base, s))
      end
    end
    if string.match(url_, "%s") then
      local base = string.match(url_, "^([^%s]+)")
      for s in string.gmatch(url_, "([^%s]+)") do
        check(urlparse.absolute(base, s))
      end
    end
    if string.match(url_, ".%%20") then
      local a, b = string.match(url_, "^(.-)%%20(.*)$")
      check(a)
      if string.len(b) > 0 then
        check(urlparse.absolute(a, b))
      end
    end
    if string.match(url_, "[%?&]fname=https?%%3[aA]%%2[fF]%%2[fF].") then
      local temp = string.match(url_, "[%?&]fname=(https?%%3[aA]%%2[fF]%%2[fF][^&;#]+)")
      if temp then
        check(urlparse.unescape(temp))
      end
    end
    if string.match(url_, "^https?://[^/]+/m/.") then
      local a, b = string.match(url_, "^(https?://[^/]+/)m/(.+)$")
      if not string.match(b, "^api/") then
        check(a .. b)
      end
    end
    if context["custom_domain"]
      and (
        (item_type == "blog" and get_domain_item(url_) == item_value)
        or (item_type == "path2" and get_domain_item(url) == context["site"])
      ) then
      local path = string.match(url_, "^https?://[^/]+(/.*)")
      local extra_domain = context["custom_domain"]
      if context["site"] == context["custom_domain"] then
        extra_domain = context["tistory_blog"] .. ".tistory.com"
      end
      check(urlparse.absolute("https://" .. extra_domain .. "/", path))
    end
    --[[if item_type == "blog"
      and string.match(url_, "^https?://[^/]+/[0-9]+$")
      and get_domain_item(url_)  then
      local post_id = tonumber(string.match(url_, "([0-9]+)$"))
      if post_id > context["max_post_id"] then
        context["max_post_id"] = post_id
        if string.match(item_value, "%.") then
          error("Did not expect custom domain.")
        end
        for i = 1 , post_id do
          check("https://" .. item_value .. ".tistory.com/" .. tostring(i))
        end
      end
    end]]
    if string.match(url, "^https?://[^/]*daumcdn%.net/cfile/tistory")
      and not string.match(url, "%?") then
      check(url .. "?original")
    end
    if not processed(url_)
      and not processed(url_ .. "/")
      and allowed(url_, origurl) then
      local headers = {}
--print('queuing', url_)
      table.insert(urls, {
        url=url_,
        headers=headers
      })
      addedtolist[url_] = true
      addedtolist[url] = true
    end
  end

  local function checknewurl(newurl)
    if not newurl then
      newurl = ""
    end
    newurl = decode_codepoint(newurl)
    if string.match(newurl, "['\"><]") then
      return nil
    end
    if string.match(newurl, "^https?:////") then
      check(string.gsub(newurl, ":////", "://"))
    elseif string.match(newurl, "^https?://") then
      check(newurl)
    elseif string.match(newurl, "^https?:\\/\\?/") then
      check(string.gsub(newurl, "\\", ""))
    elseif string.match(newurl, "^\\/\\/") then
      checknewurl(string.gsub(newurl, "\\", ""))
    elseif string.match(newurl, "^//") then
      check(urlparse.absolute(url, newurl))
    elseif string.match(newurl, "^\\/") then
      checknewurl(string.gsub(newurl, "\\", ""))
    elseif string.match(newurl, "^/") then
      check(urlparse.absolute(url, newurl))
    elseif string.match(newurl, "^%.%./") then
      if string.match(url, "^https?://[^/]+/[^/]+/") then
        check(urlparse.absolute(url, newurl))
      else
        checknewurl(string.match(newurl, "^%.%.(/.+)$"))
      end
    elseif string.match(newurl, "^%./") then
      check(urlparse.absolute(url, newurl))
    end
  end

  local function checknewshorturl(newurl)
    if not newurl then
      newurl = ""
    end
    newurl = decode_codepoint(newurl)
    if string.match(newurl, "^%?") then
      check(urlparse.absolute(url, newurl))
    elseif not (
      string.match(newurl, "^https?:\\?/\\?//?/?")
      or string.match(newurl, "^[/\\]")
      or string.match(newurl, "^%./")
      or string.match(newurl, "^[jJ]ava[sS]cript:")
      or string.match(newurl, "^[mM]ail[tT]o:")
      or string.match(newurl, "^vine:")
      or string.match(newurl, "^android%-app:")
      or string.match(newurl, "^ios%-app:")
      or string.match(newurl, "^data:")
      or string.match(newurl, "^irc:")
      or string.match(newurl, "^%${")
    ) then
      check(urlparse.absolute(url, newurl))
    end
  end

  local function set_new_params(newurl, data)
    for param, value in pairs(data) do
      if value == nil then
        value = ""
      elseif type(value) == "string" then
        value = "=" .. value
      end
      if string.match(newurl, "[%?&]" .. param .. "[=&]") then
        newurl = string.gsub(newurl, "([%?&]" .. param .. ")=?[^%?&;]*", "%1" .. value)
      else
        if string.match(newurl, "%?") then
          newurl = newurl .. "&"
        else
          newurl = newurl .. "?"
        end
        newurl = newurl .. param .. value
      end
    end
    return newurl
  end

  local function increment_param(newurl, param, default, step)
    local value = string.match(newurl, "[%?&]" .. param .. "=([0-9]+)")
    if value then
      value = tonumber(value)
      value = value + step
      return set_new_params(newurl, {[param]=tostring(value)})
    else
      if default ~= nil then
        default = tostring(default)
      end
      return set_new_params(newurl, {[param]=default})
    end
  end

  local function get_count(data)
    local count = 0
    for _ in pairs(data) do
      count = count + 1
    end
    return count
  end

  if item_type == "asset" then
    local a, b = string.match(url, "^https?://([a-z]+)[0-9]*(%.daumcdn%.net/.+)")
    if a == "t" or a == "img" or a == "tistory" or a == "i" then
      for i = 0 , 4 do
        if a == "i" and (i > 2 or i == 0) then
          i = 1
        end
        if i == 0 then
          i = ""
        else
          i = tostring(i)
        end
        if string.len(i) > 0 or a ~= "t" then
          check(urlparse.absolute(url, "//" .. a .. i .. b))
        end
      end
    end
  end

  if allowed(url)
    and status_code < 300
    and item_type ~= "asset"
    and not string.match(url, "^https?://[^/]*daumcdn%.net/")
    and not string.match(url, "^https?://[^/]*kakaocdn%.net/") then
    html = read_file(file)
    if string.match(url, "%?_version_=[0-9]+$") then
      check(string.match(url, "^([^%?]+)"))
    end
    if item_type == "path2" or item_type == "blog" then
      if not context["config"] then
        local config = string.match(html, "window%.T%.config%s*=%s*({.-});")
        if not config then
          abort_item()
          return {}
        end
        context["config"] = cjson.decode(config)
        local tistory_blog = string.gsub(string.match(html, "window%.TistoryBlog%s*=%s*({.-});"), "([a-zA-Z]+):%s*\"", "\"%1\":\"")
        context["tistory_blog"] = get_domain_item(cjson.decode(tistory_blog)["tistoryUrl"])
        if string.match(context["tistory_blog"], "%.") then
          print("Did not expect a custom domain.")
          abort_item()
          return {}
        end
        local default_url = string.match(context["config"]["DEFAULT_URL"], "^https?://([^/]+)")
        if not default_url then
          error("Could not find default URL.")
        end
        if not string.match(default_url, "%.tistory%.com$") then
          context["custom_domain"] = default_url
          sites[default_url] = true
          for newurl, parents in pairs(context["outlinks"]) do
            discovered_outlinks[string.match(percent_encode_url(newurl), "^([^%s]+)")] = nil
            for parent_url, _ in pairs(parents) do
              if parent_url == "no_parent" then
                parent_url = nil
              end
              check(newurl)
            end
          end
          context["found_domains"] = true
          context["outlinks"] = "finished"
        end
      end
      local path = string.match(url, "^https?://[^/]+(/[^%?&;]*)$")
      if path and get_domain_item(url) and not string.match(path, "^/m/") then
        check(urlparse.absolute(url, "/m" .. path))
      end
    end
    if item_type == "blog" then
      if string.match(url, "^https?://[^/]+/sitemap%.xml$") then
        local max_year = 0
        for s in string.gmatch(html, "<url>(.-)</url>") do
          local loc = string.match(s, "<loc>%s*(.-)%s*</loc>")
          local lastmod = string.match(s, "<lastmod>%s*(.-)%s*</lastmod>")
          if loc and lastmod
            and (
              (string.match(loc, "/m/") and string.match(loc, "^https?://[^/]+/m/."))
              or (not string.match(loc, "/m/") and string.match(loc, "^https?://[^/]+/."))
            ) then
            local year = string.match(lastmod, "^([0-9][0-9][0-9][0-9])%-")
            if not year then
              error("Did not find year...")
            end
            year = tonumber(year)
            if year > max_year then
              max_year = year
            end
          end
        end
        if max_year > 2022 then
          print("This blog contains new posts. Skipping...")
          local new_discovered_items = {}
          for item_name, v in pairs(discovered_items) do
            local site_name = string.match(item_name, "^path2:([^:]+):")
            if not site_name or site_name ~= context["site"] then
              new_discovered_items[item_name] = v
            end
          end
          discovered_items = new_discovered_items
          context["is_new"] = true
          return {}
        end
      end
      local blog_id = tostring(context["config"]["BLOG"]["id"])
      context["blog_id"] = blog_id
      ids[blog_id] = true
      check("https://tistory1.daumcdn.net/tistory/" .. blog_id .. "/skin/skin.html")
      if string.match(url, "^https?://[^/]+/$") then
        for _, path in pairs({
          "sitemap.xml",
          "robots.txt",
          "/m/api/blog/info",
          "/m/api/blog/init/",
          "/m/api/storyCreator",
          "/m/api/categories",
          "/m/api/blog/info",
          "/m/api/entry/0/POST?page=0&size=20",
          "/m/api/topEntries",
          "/m/api/entry/0/NOTICE?page=0&size=20",
          "/m/api/revenue/list",
        }) do
          check(urlparse.absolute(url, path))
        end
      end
    end
    if string.match(url, "/m/api/guestbook$")
      or string.match(url, "/m/api/guestbook%?")
      or string.match(url, "/m/api/[0-9]+/comment$")
      or string.match(url, "/m/api/[0-9]+/comment%?")
      or string.match(url, "/m/api/.-[%?&]page=") then
      local json = cjson.decode(html)["data"]
      if not json["isLast"] then
        local key = "nextId"
        local param_key = "startId"
        if not json[key] then
          key = "nextPage"
          param_key = "page"
        end
        if not json[key] then
          error("Could not find next page.")
        end
        check(set_new_params(url, {[param_key]=tostring(json[key])}))
      end
    end
    if item_type == "path2" then
      local temp = string.match(url, "^(.+)/m/guestbook$")
      if temp then
        for _, path in pairs({
          "/m/api/guestbook",
          "/m/api/guestbook/pin",
          "/m/api/guestbook/count",
          "/m/api/guestbook/config"
        }) do
          check(temp .. path)
        end
      end
      if not context["published_time"] then
        local published_time = string.match(html, "<meta property=\"article:published_time\" content=\"([0-9][0-9][0-9][0-9]%-[0-9][0-9])")
        if published_time then
          context["published_time"] = published_time
          local year, month = string.match(published_time, "^([0-9]+)%-([0-9]+)$")
          check(urlparse.absolute(url, "/archive/" .. year .. month))
        elseif context["post_id"] then
          error("Expected to find a publishing time...")
        end
      end
      if string.match(url, "^https?://[^/]+/m/[0-9]+$") then
        for _, path in pairs({
          --"/m/api/entry/" .. context["post_id"] .. "/related",
          --"/m/api/entry/" .. context["post_id"] .. "/popular",
          "/m/api/" .. context["post_id"] .. "/comment/count",
          "/m/api/" .. context["post_id"] .. "/comment/config",
          "/m/api/" .. context["post_id"] .. "/comment/pin",
          "/m/api/" .. context["post_id"] .. "/comment",
          "/m/api/" .. context["post_id"] .. "/comment?reverse=true",
          "/m/api/" .. context["post_id"] .. "/reaction",
          "/reaction?entryId" .. context["post_id"]
        }) do
          check(urlparse.absolute(url, path))
        end
      end
      if string.match(url, "^https?://[^/]+/[0-9]+$") then
        check(urlparse.absolute(url, "/reaction?entryId=" .. context["post_id"]))
      end
    end
    for newurl in string.gmatch(string.gsub(html, "&[qQ][uU][oO][tT];", '"'), '([^"]+)') do
      checknewurl(newurl)
    end
    for newurl in string.gmatch(string.gsub(html, "&#039;", "'"), "([^']+)") do
      checknewurl(newurl)
    end
    for newurl in string.gmatch(html, "[^%-]href='([^']+)'") do
      checknewshorturl(newurl)
    end
    for newurl in string.gmatch(html, '[^%-]href="([^"]+)"') do
      checknewshorturl(newurl)
    end
    for newurl in string.gmatch(html, "url%(([^%)]+)%)") do
      checknewurl(newurl)
    end
    html = string.gsub(html, "&gt;", ">")
    html = string.gsub(html, "&lt;", "<")
    for newurl in string.gmatch(html, ">%s*([^<%s]+)") do
      checknewurl(newurl)
    end
  end

  return urls
end

wget.callbacks.write_to_warc = function(url, http_stat)
  status_code = http_stat["statcode"]
  set_item(url["url"])
  url_count = url_count + 1
  io.stdout:write(url_count .. "=" .. status_code .. " " .. url["url"] .. " \n")
  io.stdout:flush()
  logged_response = true
  if not item_name then
    error("No item name found.")
  end
  is_initial_url = false
  if http_stat["statcode"] ~= 200
    and http_stat["statcode"] ~= 301
    and http_stat["statcode"] ~= 302
    and http_stat["statcode"] ~= 404 then
    retry_url = true
    return false
  end
  if http_stat["len"] == 0
    and http_stat["statcode"] < 300 then
    retry_url = true
    return false
  end
  if abortgrab then
    print("Not writing to WARC.")
    return false
  end
  retry_url = false
  tries = 0
  return true
end

wget.callbacks.httploop_result = function(url, err, http_stat)
  status_code = http_stat["statcode"]

  if not logged_response then
    url_count = url_count + 1
    io.stdout:write(url_count .. "=" .. status_code .. " " .. url["url"] .. " \n")
    io.stdout:flush()
  end
  logged_response = false

  if killgrab then
    return wget.actions.ABORT
  end

  set_item(url["url"])
  if not item_name then
    error("No item name found.")
  end

  if abortgrab then
    abort_item()
    return wget.actions.EXIT
  end

  if status_code == 0 or retry_url then
    io.stdout:write("Server returned bad response. ")
    io.stdout:flush()
    tries = tries + 1
    local maxtries = 2
    if status_code == 429 then
      maxtries = 10
    elseif status_code == 403 then
      tries = maxtries + 1
    end
    if tries > maxtries then
      io.stdout:write(" Skipping.\n")
      io.stdout:flush()
      tries = 0
      abort_item()
      return wget.actions.EXIT
    end
    local factor = 2
    local sleep_time = math.random(
      math.floor(math.pow(factor, tries-0.5)),
      math.floor(math.pow(factor, tries))
    )
    io.stdout:write("Sleeping " .. sleep_time .. " seconds.\n")
    io.stdout:flush()
    os.execute("sleep " .. sleep_time)
    return wget.actions.CONTINUE
  else
    if status_code == 200 then
      if not seen_200[url["url"]] then
        seen_200[url["url"]] = 0
      end
      seen_200[url["url"]] = seen_200[url["url"]] + 1
    end
    downloaded[url["url"]] = true
  end

  if status_code >= 300 and status_code <= 399 then
    local newloc = urlparse.absolute(url["url"], http_stat["newloc"])
    if not allowed(newloc, url["url"]) or processed(newloc) then
      tries = 0
      return wget.actions.EXIT
    end
  end

  tries = 0

  return wget.actions.NOTHING
end

wget.callbacks.finish = function(start_time, end_time, wall_time, numurls, total_downloaded_bytes, total_download_time)
  local function submit_backfeed(items, key)
    local tries = 0
    local maxtries = 5
    while tries < maxtries do
      if killgrab then
        return false
      end
      local body, code, headers, status = http.request(
        "https://legacy-api.arpa.li/backfeed/legacy/" .. key,
        items .. "\0"
      )
      if code == 200 and body ~= nil and cjson.decode(body)["status_code"] == 200 then
        io.stdout:write(string.match(body, "^(.-)%s*$") .. "\n")
        io.stdout:flush()
        return nil
      end
      io.stdout:write("Failed to submit discovered URLs." .. tostring(code) .. tostring(body) .. "\n")
      io.stdout:flush()
      os.execute("sleep " .. math.floor(math.pow(2, tries)))
      tries = tries + 1
    end
    kill_grab()
    error()
  end

  local file = io.open(item_dir .. "/" .. warc_file_base .. "_bad-items.txt", "w")
  for url, _ in pairs(bad_items) do
    file:write(url .. "\n")
  end
  file:close()
  for key, data in pairs({
    ["tistory-4ccqwwcio39imora"] = discovered_items,
    ["urls-stash-tistory-xsk2xy8djiohujgu"] = discovered_outlinks
  }) do
    print("queuing for", string.match(key, "^(.+)%-"))
    local items = nil
    local count = 0
    for item, _ in pairs(data) do
      print("found item", item)
      if items == nil then
        items = item
      else
        items = items .. "\0" .. item
      end
      count = count + 1
      if count == 1000 then
        submit_backfeed(items, key)
        items = nil
        count = 0
      end
    end
    if items ~= nil then
      submit_backfeed(items, key)
    end
  end
end

wget.callbacks.before_exit = function(exit_status, exit_status_string)
  if killgrab then
    return wget.exits.IO_FAIL
  end
  if abortgrab then
    abort_item()
  end
  return exit_status
end


