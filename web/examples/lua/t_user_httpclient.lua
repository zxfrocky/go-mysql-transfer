local ops = require("scriptOps") --加载脚本操作模块
local json = require("json")   -- 加载json模块
local httpclient = require("httpOps") --加载http操作模块

local row = ops.rawRow()  --数据库当前变更的一行数据,table类型，key为列名称
local action = ops.rawAction()  --当前数据库事件,包括：insert、updare、delete

local _id = row["ID"] --获取ID列的值
local _userName = row["USER_NAME"] --获取USER_NAME列的值

if action == "insert" then -- 插入事件
	-- http get请求，第一个参数为URL,类型为string;第二个参数为header参数，类型为table
	--local url = string.format("http://localhost:8080/users/%s", _userName) 
	--local res = httpclient.get(url,{
	--	Authorization="Basic OSdjJGRpbjpvcGVuIANlc2SdDE=="
	--}) 
	--if(res~=nil) then 
		-- http响应代码，如：200、401、403、500等
	--	print(res.status_code)  
		-- http响应内容，string类型
	--	print(res.body)
	--else 
	--	print("request fail")
	--end

	--http post请求，第一个参数为URL,类型为string;第二个参数为header参数，类型为table;第三个参数为post内容，类型为table
	local url = "http://localhost:8080/users"
	local _data = json.encode(row) -- 将result转为json
	local res = httpclient.post(url,{
			Authorization="Basic OSdjJGRpbjpvcGVuIANlc2SdDE=="
		},{
			data=_data,
		})
	if(res~=nil) then 
		-- http响应代码，如：200、401、403、500等
		print(res.status_code)  
		-- http响应内容，string类型
		print(res.body)
	else 
		print("post request fail")
	end
end 

if action == "update" then -- 更新事件
	--http put请求，第一个参数为URL,类型为string;第二个参数为header参数，类型为table;第三个参数为put内容，类型为table
	local url = "http://localhost:8080/users"
	local _data = json.encode(row) -- 将result转为json
	local res = httpclient.put(url,{
			Authorization="Basic OSdjJGRpbjpvcGVuIANlc2SdDE=="
		},{
			data=_data,
		})
	if(res~=nil) then 
		-- http响应代码，如：200、401、403、500等
		print(res.status_code)  
		-- http响应内容，string类型
		print(res.body)
	else 
		print("put request fail")
	end
end 

if action == "delete" then -- 删除事件
	-- http delete请求，第一个参数为URL,类型为string;第二个参数为header参数，类型为table
	local url = string.format("http://localhost:8080/users/%s", _id) 
	local res = httpclient.delete(url,{
		Authorization="Basic OSdjJGRpbjpvcGVuIANlc2SdDE=="
	}) 
	if(res~=nil) then 
		-- http响应代码，如：200、401、403、500等
		print(res.status_code)  
		-- http响应内容，string类型
		print(res.body)
	else 
		print("delete request fail")
	end
end 