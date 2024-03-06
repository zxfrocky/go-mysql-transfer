local ops = require("redisOps") --加载redis操作模块
local json = require("json")   --加载json模块
local db = require("dbOps") --加载数据库操作模块

local row = ops.rawRow()  --当前数据库的一行数据,table类型，key为列名称
local action = ops.rawAction()  --当前数据库事件,包括：insert、updare、delete

local result = {}  -- 定义一个table,作为结果
if action == "insert" then -- 只监听插入事件 SADD(
	local areaId = row["AREA_ID"] --获取区域AREA_ID列的值
	-- SQL语句，不能直接使用表名，要使用(数据库名称.表名称)，如：ESEAP.T_AREA
	local sql = string.format("SELECT * FROM ESEAP.T_AREA WHERE ID = '%s'",areaId) 
	-- 执行SQL查询，返回一条查询结果，table类型;结构如：{"ID":"340100000000","NAME":"安徽省 合肥市"}
	local rs = db.selectOne(sql) 
	local areaName = rs["NAME"] --区域名称
	
	local id = row["ID"] --获取row ID列的值
	result["id"] = id
	result["action"] = action
	result["userName"] = row["USER_NAME"] --获取row USER_NAME列的值，赋给结果集
	result["password"] = row["PASSWORD"] --获取row USER_NAME列的值，赋给结果集
	result["createTime"] = row["CREATE_TIME"] --获取row CREATE_TIME列的值，赋给结果集
	result["source"] = "binlog" -- 数据来源
	result["areaId"] = areaId
	result["areaName"] = areaName
	local key = "db_lua_test_"..id -- 定义key
	local val = json.encode(result) -- 将result转为json
	ops.SET(key,val) -- 对应Redis的SET命令，第一个参数为key(支持string类型)，第二个参数为value
end 