local json = require("json")   -- 加载json模块
local ops = require("redisOps") --加载redis操作模块

local row = ops.rawRow()  --数据库当前变更的一行数据,table类型，key为列名称
local action = ops.rawAction()  --当前数据库事件,包括：insert、updare、delete

local id = row["ID"] --获取ID列的值
local userName = row["USER_NAME"] --获取USER_NAME列的值
local key = "user_"..id -- 定义key

if action == "delete" -- 删除事件
then
	ops.DEL(key) 
	ops.SREM("user_set",userName) 
else 
	local password = row["PASSWORD"] --获取USER_NAME列的值
	local createTime = row["CREATE_TIME"] --获取CREATE_TIME列的值
	local result = {}  -- 定义一个table
	result["id"] = id
	result["userName"] = userName
	result["password"] = password
	result["createTime"] = createTime
	result["source"] = "binlog" -- 数据来源
	local val = json.encode(result) -- 将newTable转为json
	ops.SET(key,val) -- 对应Redis的SET命令，第一个参数为key(支持string类型)，第二个参数为value
	
	if action == "update" -- 修改事件
	then
		local oldRow = ops.rawOldRow()  --数据库变更之前的数据(修改之前的数据)
		local oldUserName = oldRow["USER_NAME"] --获取USER_NAME列的值
		ops.SREM("user_set",oldUserName) -- 删除旧值
	end
	
	ops.SADD("user_set",userName) -- 对应Redis的SADD命令，第一个参数为key(支持string类型)，第二个参数为value
end 