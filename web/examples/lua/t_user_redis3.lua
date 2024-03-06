local ops = require("redisOps") --加载redis操作模块

local row = ops.rawRow()  --数据库当前变更的一行数据,table类型，key为列名称
local action = ops.rawAction()  --当前数据库事件,包括：insert、updare、delete

local userName = row["USER_NAME"] --获取USER_NAME列的值
local createTime = row["CREATE_TIME"] --获取CREATE_TIME列的值,13位时间戳

local key = "users"

if action == "insert" -- 监听insert事件
then
	ops.ZADD(key, createTime, userName)
elseif action == "update" -- 监听update事件
then
	local oldRow = ops.rawOldRow()  --数据库变更之前的数据(修改之前的数据)
	local oldUserName = oldRow["USER_NAME"] --获取USER_NAME列的值
	ops.ZREM(key,oldUserName) -- 删除旧值
	ops.ZADD(key, createTime, userName) --插入新值
else -- 监听删除事件
	ops.ZREM(key,userName) -- 删除旧值
end