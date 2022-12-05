using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection.Emit;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;
using MongoDB.Driver;
using MongoDB.Bson;
using MongoDB.Bson.IO;
using System.Data;
using System.Reflection;
using Lead.Tool.Log;

namespace Lead.Tool.MongoDB
{
    public class MongoHelper
    {
        private string _connStr = "mongodb://192.168.1.21:27017";
        private string _databaseName = "Lead";
        private string _collectionName = "parts";
        private bool _initedError;
        public MongoClient _client;
        public IMongoDatabase _database;

        public MongoHelper()
        {

        }
        public MongoHelper(string connStr, string dbName)
        {
            _connStr = connStr;
            _databaseName = dbName;
            try
            {
                _client = new MongoClient(_connStr);
                _database = _client.GetDatabase(_databaseName);
                _initedError = false;
            }
            catch (Exception e)
            {
                _initedError = true;
                Console.WriteLine(e);
                throw;
            }
        }

        public int MongodbHelperSet(string connStr, string dbName)
        {
            int iRet = 0;
            _connStr = connStr;
            _databaseName = dbName;
            try
            {
                _client = new MongoClient(_connStr);
                _database = _client.GetDatabase(_databaseName);
                _initedError = false;
            }
            catch (Exception e)
            {
                _initedError = true;
                Console.WriteLine(e);
                return -1;
            }

            return iRet;
        }
        public MongoHelper(string connStr, string dbName, string collectionName)
        {
            _connStr = connStr;
            _databaseName = dbName;
            _collectionName = collectionName;
            try
            {
                _client = new MongoClient(_connStr);
                _database = _client.GetDatabase(_databaseName);
                _initedError = false;
            }
            catch (Exception e)
            {
                _initedError = true;
                Console.WriteLine(e);
                throw;
            }
        }
        #region 插入单条数据
        /// <summary>
        /// 插入单条数据
        /// </summary>
        /// <typeparam name="T">类型</typeparam>
        /// <param name="entity">要插入的实体对象</param>
        /// <returns>0-成功</returns>
        public int InsertOne<T>(T entity) where T : class
        {
            if (_initedError || _client == null || _database == null)
            {
                return -1;
            }

            var collection = _database.GetCollection<T>(_collectionName);
            try
            {
                collection.InsertOne(entity);
                return 0;
            }
            catch (Exception e)
            {
                return -2;
            }
        }
        public int InsertOne<T>(T entity, string collectionName) where T : class
        {
            if (_initedError || _client == null || _database == null)
            {
                return -1;
            }

            var collection = _database.GetCollection<T>(collectionName);
            try
            {
                collection.InsertOne(entity);
                return 0;
            }
            catch (Exception e)
            {
                return -2;
            }
        }
        /// <summary>
        /// 插入单条数据（异步操作）
        /// </summary>
        /// <typeparam name="T">类型</typeparam>
        /// <param name="entity">要插入的实体对象</param>
        /// <returns>0-成功</returns>
        public async Task<int> InsertOneAsync<T>(T entity) where T : class
        {
            if (_initedError || _client == null || _database == null)
            {
                return -1;
            }

            var collection = _database.GetCollection<T>(_collectionName);
            try
            {
                await collection.InsertOneAsync(entity);
                return 0;
            }
            catch (Exception e)
            {
                return -2;
            }
        }
        public async Task<int> InsertOneAsync<T>(T entity, string collectionName,string ID) where T : class
        {
            if (_initedError || _client == null || _database == null)
            {
                return -1;
            }

            var collection = _database.GetCollection<T>(collectionName);
            try
            {
                await collection.InsertOneAsync(entity);
                Logger.Info(ID+" 保存数据库成功");
                return 0;
            }
            catch (Exception e)
            {
                Logger.Info(ID + " 保存数据库失败："+e.Message);
                Logger.Error(collectionName+ " collectionName Error");

                {
                    var collection1 = _database.GetCollection<T>(collectionName);
                    try
                    {
                        await collection1.InsertOneAsync(entity);
                        Logger.Info(ID + " 第二次保存数据库成功");
                        return 0;
                    }
                    catch (Exception e1)
                    {
                        Logger.Info(ID + " 第二次保存数据库失败：" + e1.Message);
                    }
                }
                return -2;
            }
        }
        #endregion
        #region 插入多组数据
        /// <summary>
        /// 插入多组数据
        /// </summary>
        /// <typeparam name="T">类型</typeparam>
        /// <param name="entityList">要插入的实体对象列表</param>
        /// <returns>0-成功</returns>
        public int InsertMany<T>(List<T> entityList) where T : class
        {
            if (_initedError || _client == null || _database == null)
            {
                return -1;
            }

            var collection = _database.GetCollection<T>(_collectionName);
            try
            {
                collection.InsertMany(entityList);
                return 0;
            }
            catch (Exception e)
            {
                return -2;
            }
        }
        public int InsertMany<T>(List<T> entityList, string collectionName) where T : class
        {
            if (_initedError || _client == null || _database == null)
            {
                return -1;
            }

            var collection = _database.GetCollection<T>(collectionName);
            try
            {
                collection.InsertMany(entityList);
                return 0;
            }
            catch (Exception e)
            {
                return -2;
            }
        }
        /// <summary>
        /// 插入多组数据（异步操作）
        /// </summary>
        /// <typeparam name="T">类型</typeparam>
        /// <param name="entityList">要插入的实体对象列表</param>
        /// <returns>0-成功</returns>
        public async Task<int> InsertManyAsync<T>(List<T> entityList) where T : class
        {
            if (_initedError || _client == null || _database == null)
            {
                return -1;
            }

            var collection = _database.GetCollection<T>(_collectionName);
            try
            {
                await collection.InsertManyAsync(entityList);
                return 0;
            }
            catch (Exception e)
            {
                return -2;
            }
        }

        public async Task<int> InsertManyAsync<T>(List<T> entityList, string collectionName) where T : class
        {
            if (_initedError || _client == null || _database == null)
            {
                return -1;
            }

            var collection = _database.GetCollection<T>(collectionName);
            try
            {
                await collection.InsertManyAsync(entityList);
                return 0;
            }
            catch (Exception e)
            {
                return -2;
            }
        }
        #endregion

        #region 按条件查询单个数据
        /// <summary>
        /// 给定单组条件查询单组数据(第一个)
        /// </summary>
        /// <typeparam name="T">类型</typeparam>
        /// <param name="fieldName">查询字段名</param>
        /// <param name="value">查询字段值</param>
        /// <param name="result">查询到的对象，没有或出错则为null</param>
        /// <returns>0-成功</returns>
        public int QueryOneFirst<T>(string fieldName, string value, out T result) where T : class
        {
            if (_initedError || _client == null || _database == null)
            {
                result = null;
                return -1;
            }

            var collection = _database.GetCollection<T>(_collectionName);
            var filter = Builders<T>.Filter.Eq(fieldName, value);
            var projection = Builders<T>.Projection.Exclude("_id");
            var findOption = new FindOptions
            {
                AllowPartialResults = true,
                Collation = Collation.Simple,
                BatchSize = Int32.MaxValue,
            };

            try
            {
                var document = collection.Find<T>(filter, findOption).Project<T>(projection);
                result = document.First();
                return 0;
            }
            catch (Exception e)
            {
                result = null;
                return -2;
            }
        }

        /// <summary>
        /// 给定单组条件查询单组数据(最后一个)
        /// </summary>
        /// <typeparam name="T">类型</typeparam>
        /// <param name="fieldName">查询字段名</param>
        /// <param name="value">查询字段值</param>
        /// <param name="result">查询到的对象，没有或出错则为null</param>
        /// <returns>0-成功</returns>
        public int QueryOneLast<T>(string fieldName, string value, out T result) where T : class
        {
            if (_initedError || _client == null || _database == null)
            {
                result = null;
                return -1;
            }

            var collection = _database.GetCollection<T>(_collectionName);
            var filter = Builders<T>.Filter.Eq(fieldName, value);
            var projection = Builders<T>.Projection.Exclude("_id");
            var findOption = new FindOptions
            {
                AllowPartialResults = true,
                Collation = Collation.Simple,
                BatchSize = Int32.MaxValue,
            };

            try
            {
                var document = collection.Find<T>(filter, findOption).Project<T>(projection);
                List<T> list = document.ToList();
                result = list.Last();
                return 0;
            }
            catch (Exception e)
            {
                result = null;
                return -2;
            }
        }
        #endregion

        #region 查询多组数据
        /// <summary>
        /// 查询所有数据
        /// </summary>
        /// <typeparam name="T">类型</typeparam>
        /// <param name="result">查询结果</param>
        /// <returns>0-成功</returns>
        public int QueryAll<T>(out List<T> result) where T : class
        {
            if (_initedError || _client == null || _database == null)
            {
                result = null;
                return -1;
            }

            var collection = _database.GetCollection<T>(_collectionName);
            var filter = Builders<T>.Filter.Ne("_id", "");
            var sort = Builders<T>.Sort.Descending("CreateTime");
            var projection = Builders<T>.Projection.Exclude("_id");

            try
            {
                var document = collection.Find<T>(filter).Project<T>(projection).Sort(sort);
                result = document.ToList();
                return 0;
            }
            catch (Exception e)
            {
                result = null;
                return -2;
            }
        }
        /// <summary>
        /// 给定单一条件查询数据
        /// </summary>
        /// <typeparam name="T">类型</typeparam>
        /// <param name="fieldName">字段名称</param>
        /// <param name="value">字段值</param>
        /// <param name="result">查询结果</param>
        /// <returns>0-成功</returns>
        public int QueryMany<T>(string fieldName, string value, out List<T> result) where T : class
        {
            if (_initedError || _client == null || _database == null)
            {
                result = null;
                return -1;
            }

            var collection = _database.GetCollection<T>(_collectionName);
            var filter = Builders<T>.Filter.Eq(fieldName, value);
            var sort = Builders<T>.Sort.Descending("CreateTime");
            var projection = Builders<T>.Projection.Exclude("_id");
            try
            {
                var document = collection.Find<T>(filter).Project<T>(projection).Sort(sort);
                result = document.ToList();
                return 0;
            }
            catch (Exception e)
            {
                result = null;
                return -2;
            }
        }
        /// <summary>
        /// 给定多组条件查询数据
        /// </summary>
        /// <typeparam name="T">类型</typeparam>
        /// <param name="fieldArray">字段名称数组</param>
        /// <param name="valueArray">字段值数组</param>
        /// <param name="result">查询结果</param>
        /// <returns>0-成功</returns>
        public int QueryMany<T>(string[] fieldArray, string[] valueArray, out List<T> result) where T : class
        {
            if (_initedError || _client == null || _database == null || fieldArray.Length != valueArray.Length)
            {
                result = null;
                return -1;
            }

            var collection = _database.GetCollection<T>(_collectionName);

            var subFilters = new FilterDefinition<T>[fieldArray.Length];
            for (var i = 0; i < subFilters.Length; i++)
            {
                subFilters[i] = Builders<T>.Filter.Eq(fieldArray[i], valueArray[i]);
            }
            var filter = Builders<T>.Filter.And(subFilters);

            var sort = Builders<T>.Sort.Descending("CreateTime");
            var projection = Builders<T>.Projection.Exclude("_id");
            try
            {
                var document = collection.Find<T>(filter).Project<T>(projection).Sort(sort);
                result = document.ToList();
                return 0;
            }
            catch (Exception e)
            {
                result = null;
                return -2;
            }
        }
        /// <summary>
        /// 通过起止时间查询数据
        /// </summary>
        /// <typeparam name="T">类型</typeparam>
        /// <param name="startTime">起始时间（可为null）</param>
        /// <param name="endTime">终止时间（可为null）</param>
        /// <param name="result">查询结果</param>
        /// <returns>0-成功</returns>
        public int QueryManyByTime<T>(string startTime, string endTime, out List<T> result) where T : class
        {
            if (_initedError || _client == null || _database == null)
            {
                result = null;
                return -1;
            }

            if (startTime == null)
            {
                startTime = "2000-01-01 01:01:01Z"; //DateTime.ToString("u")
            }

            if (endTime == null)
            {
                endTime = "2200-01-01 01:01:01Z";   //DateTime.ToString("u")
            }

            var start = Convert.ToDateTime(startTime);
            var end = Convert.ToDateTime(endTime);

            var collection = _database.GetCollection<T>(_collectionName);
            var filterBuilder = Builders<T>.Filter;
            var filter = filterBuilder.Gte("CreateTime", start) & filterBuilder.Lte("CreateTime", end);
            var sort = Builders<T>.Sort.Descending("CreateTime");
            var projection = Builders<T>.Projection.Exclude("_id");
            try
            {
                var document = collection.Find<T>(filter).Project<T>(projection).Sort(sort);
                result = document.ToList();
                return 0;
            }
            catch (Exception e)
            {
                result = null;
                return -2;
            }

        }
        /// <summary>
        /// 通过起止时间查询数据
        /// </summary>
        /// <typeparam name="T">类型（必须包含CreateTime字段）</typeparam>
        /// <param name="startTime">起始时间</param>
        /// <param name="endTime">终止时间）</param>
        /// <param name="result">查询结果</param>
        /// <returns>0-成功</returns>
        public int QueryManyByTime<T>(DateTime startTime, DateTime endTime, out List<T> result) where T : class
        {
            if (_initedError || _client == null || _database == null)
            {
                result = null;
                return -1;
            }

            var collection = _database.GetCollection<T>(_collectionName);
            var filterBuilder = Builders<T>.Filter;
            var filter = filterBuilder.Gte("CreateTime", startTime) & filterBuilder.Lte("CreateTime", endTime);
            var sort = Builders<T>.Sort.Descending("CreateTime");
            var projection = Builders<T>.Projection.Exclude("_id");
            try
            {
                var document = collection.Find<T>(filter).Project<T>(projection).Sort(sort);
                result = document.ToList();
                return 0;
            }
            catch (Exception e)
            {
                result = null;
                return -2;
            }

        }
        public int QueryManyByTime<T>(DateTime startTime, DateTime endTime, out List<T> result, string collectionName) where T : class
        {
            if (_initedError || _client == null || _database == null)
            {
                result = null;
                return -1;
            }

            var collection = _database.GetCollection<T>(collectionName);
            var filterBuilder = Builders<T>.Filter;
            var filter = filterBuilder.Gte("CreateTime", startTime) & filterBuilder.Lte("CreateTime", endTime);
            var sort = Builders<T>.Sort.Descending("CreateTime");
            var projection = Builders<T>.Projection.Exclude("_id");
            try
            {
                var document = collection.Find<T>(filter).Project<T>(projection).Sort(sort);
                result = document.ToList();
                return 0;
            }
            catch (Exception e)
            {
                result = null;
                return -2;
            }

        }
        /// <summary>
        /// 给定多组条件查询数据并指定起止时间
        /// </summary>
        /// <typeparam name="T">类型（必须包含CreateTime字段）</typeparam>
        /// <param name="fieldArray">字段名称数组</param>
        /// <param name="valueArray">字段值数组</param>
        /// <param name="startTime">起始时间（可为null）</param>
        /// <param name="endTime">终止时间（可为null）</param>
        /// <param name="result">查询结果</param>
        /// <returns>0-成功</returns>
        public int QueryManyByTime<T>(string[] fieldArray, string[] valueArray, string startTime, string endTime, out List<T> result) where T : class
        {
            if (_initedError || _client == null || _database == null || fieldArray.Length != valueArray.Length)
            {
                result = null;
                return -1;
            }


            if (startTime == null)
            {
                startTime = "2000-01-01 01:01:01Z"; //DateTime.ToString("u")
            }

            if (endTime == null)
            {
                endTime = "2200-01-01 01:01:01Z";   //DateTime.ToString("u")
            }

            var start = Convert.ToDateTime(startTime);
            var end = Convert.ToDateTime(endTime);
            var filterTime = Builders<T>.Filter.Gte("CreateTime", start) & Builders<T>.Filter.Lte("CreateTime", end);

            var collection = _database.GetCollection<T>(_collectionName);

            var subFilters = new FilterDefinition<T>[fieldArray.Length + 1];
            for (var i = 0; i < subFilters.Length; i++)
            {
                subFilters[i] = Builders<T>.Filter.Eq(fieldArray[i], valueArray[i]);
            }

            subFilters[fieldArray.Length] = filterTime;

            var filter = Builders<T>.Filter.And(subFilters);

            var sort = Builders<T>.Sort.Descending("CreateTime");
            var projection = Builders<T>.Projection.Exclude("_id");
            try
            {
                var document = collection.Find<T>(filter).Project<T>(projection).Sort(sort);
                result = document.ToList();
                return 0;
            }
            catch (Exception e)
            {
                result = null;
                return -2;
            }
        }
        /// <summary>
        /// 给定多组条件查询数据并指定起止时间
        /// </summary>
        /// <typeparam name="T">类型（必须包含CreateTime字段）</typeparam>
        /// <param name="fieldArray">字段名称数组</param>
        /// <param name="valueArray">字段值数组</param>
        /// <param name="startTime">起始时间</param>
        /// <param name="endTime">终止时间</param>
        /// <param name="result">查询结果</param>
        /// <returns>0-成功</returns>
        public int QueryManyByTime<T>(string[] fieldArray, string[] valueArray, DateTime startTime, DateTime endTime, out List<T> result) where T : class
        {
            if (_initedError || _client == null || _database == null || fieldArray.Length != valueArray.Length)
            {
                result = null;
                return -1;
            }

            var filterTime = Builders<T>.Filter.Gte("CreateTime", startTime) & Builders<T>.Filter.Lte("CreateTime", endTime);

            var collection = _database.GetCollection<T>(_collectionName);

            var subFilters = new FilterDefinition<T>[fieldArray.Length + 1];
            for (var i = 0; i < subFilters.Length; i++)
            {
                subFilters[i] = Builders<T>.Filter.Eq(fieldArray[i], valueArray[i]);
            }

            subFilters[fieldArray.Length] = filterTime;

            var filter = Builders<T>.Filter.And(subFilters);

            var sort = Builders<T>.Sort.Descending("CreateTime");
            var projection = Builders<T>.Projection.Exclude("_id");
            try
            {
                var document = collection.Find<T>(filter).Project<T>(projection).Sort(sort);
                result = document.ToList();
                return 0;
            }
            catch (Exception e)
            {
                result = null;
                return -2;
            }
        }

        public int QueryManyByTime<T>(string[] fieldArray, string[] valueArray, DateTime startTime, DateTime endTime, string collectionName, out List<T> result) where T : class
        {
            if (_initedError || _client == null || _database == null || fieldArray.Length != valueArray.Length)
            {
                result = null;
                return -1;
            }

            var filterTime = Builders<T>.Filter.Gte("CreateTime", startTime) & Builders<T>.Filter.Lte("CreateTime", endTime);

            var collection = _database.GetCollection<T>(collectionName);

            var subFilters = new FilterDefinition<T>[fieldArray.Length + 1];
            for (var i = 0; i < fieldArray.Length; i++)
            {
                subFilters[i] = Builders<T>.Filter.Eq(fieldArray[i], valueArray[i]);
            }

            subFilters[fieldArray.Length] = filterTime;

            var filter = Builders<T>.Filter.And(subFilters);

            var sort = Builders<T>.Sort.Descending("CreateTime");
            var projection = Builders<T>.Projection.Exclude("_id");
            try
            {
                var document = collection.Find<T>(filter).Project<T>(projection).Sort(sort);
                result = document.ToList();
                return 0;
            }
            catch (Exception e)
            {
                result = null;
                return -2;
            }
        }
        #endregion

        #region 更新数据
        /// <summary>
        /// 更新一条数据
        /// </summary>
        /// <typeparam name="T">类型</typeparam>
        /// <param name="chosedField">查询数据的字段名</param>
        /// <param name="chosedValue">查询数据的字段值</param>
        /// <param name="updateField">更新数据的字段名</param>
        /// <param name="updateValue">更新数据的字段值</param>
        /// <returns>0-成功</returns>
        public int UpdateOne<T>(string chosedField, string chosedValue, string updateField, string updateValue) where T : class
        {
            if (_initedError || _client == null || _database == null)
            {
                return -1;
            }

            var collection = _database.GetCollection<T>(_collectionName);
            var filter = Builders<T>.Filter.Eq(chosedField, chosedValue);
            var updated = Builders<T>.Update.Set(updateField, updateValue);

            try
            {
                var result = collection.UpdateOneAsync(filter, updated).Result;
                if (!result.IsAcknowledged)
                {
                    return -3;
                }
                return 0;
            }
            catch (Exception e)
            {
                return -2;
            }
        }
        /// <summary>
        /// 更新所有相应数据
        /// </summary>
        /// <typeparam name="T">类型</typeparam>
        /// <param name="chosedField">查询数据的字段名</param>
        /// <param name="chosedValue">查询数据的字段值</param>
        /// <param name="updateField">更新数据的字段名</param>
        /// <param name="updateValue">更新数据的字段值</param>
        /// <returns>0-成功</returns>
        public int UpdateMany<T>(string chosedField, string chosedValue, string updateField, string updateValue) where T : class
        {
            if (_initedError || _client == null || _database == null)
            {
                return -1;
            }

            var collection = _database.GetCollection<T>(_collectionName);
            var filter = Builders<T>.Filter.Eq(chosedField, chosedValue);
            var updated = Builders<T>.Update.Set(updateField, updateValue);

            try
            {
                var result = collection.UpdateManyAsync(filter, updated).Result;
                if (!result.IsAcknowledged)
                {
                    return -3;
                }
                return 0;
            }
            catch (Exception e)
            {
                return -2;
            }
        }
        /// <summary>
        /// 替换一条数据（如对象未存在，则新建）
        /// </summary>
        /// <typeparam name="T">类型</typeparam>
        /// <param name="chosedField">查询数据的字段名</param>
        /// <param name="chosedValue">查询数据的字段值</param>
        /// <param name="entiy">新的对象</param>
        /// <returns>0-成功</returns>
        public int ReplaceOne<T>(string chosedField, string chosedValue, T entiy) where T : class
        {
            if (_initedError || _client == null || _database == null)
            {
                return -1;
            }

            var collection = _database.GetCollection<T>(_collectionName);
            var filter = Builders<T>.Filter.Eq(chosedField, chosedValue);

            try
            {
                var result = collection.ReplaceOne(filter, entiy);
                if (!result.IsAcknowledged)
                {
                    return -3;
                }
                return 0;
            }
            catch (Exception e)
            {
                return -2;
            }
        }
        #endregion

        #region 删除数据
        /// <summary>
        /// 按条件删除单条数据
        /// </summary>
        /// <typeparam name="T">类型</typeparam>
        /// <param name="fieldName">字段名</param>
        /// <param name="value">字段值</param>
        /// <returns>0-成功</returns>
        public int DeleteOne<T>(string fieldName, string value) where T : class
        {
            if (_initedError || _client == null || _database == null)
            {
                return -1;
            }

            var collection = _database.GetCollection<T>(_collectionName);
            var filter = Builders<T>.Filter.Eq(fieldName, value);

            try
            {
                var result = collection.DeleteOneAsync(filter).Result;
                if (result == DeleteResult.Unacknowledged.Instance)
                {
                    return -3;
                }
                return 0;
            }
            catch (Exception e)
            {
                return -2;
            }
        }
        /// <summary>
        /// 按条件删除多组数据
        /// </summary>
        /// <typeparam name="T">类型</typeparam>
        /// <param name="fieldName">字段名</param>
        /// <param name="value">字段值</param>
        /// <returns>0-成功</returns>
        public int DeleteMany<T>(string fieldName, string value) where T : class
        {
            if (_initedError || _client == null || _database == null)
            {
                return -1;
            }

            var collection = _database.GetCollection<T>(_collectionName);
            var filter = Builders<T>.Filter.Eq(fieldName, value);

            try
            {
                var result = collection.DeleteManyAsync(filter).Result;
                if (result == DeleteResult.Unacknowledged.Instance)
                {
                    return -3;
                }
                return 0;
            }
            catch (Exception e)
            {
                return -2;
            }
        }
        /// <summary>
        /// 通过起止时间删除数据
        /// </summary>
        /// <typeparam name="T">类型（必须包含CreateTime字段）</typeparam>
        /// <param name="startTime">起始时间（可为null）</param>
        /// <param name="endTime">终止时间（可为null）</param>
        /// <returns>0-成功</returns>
        public int DeleteAllByTime<T>(string startTime, string endTime) where T : class
        {
            if (_initedError || _client == null || _database == null)
            {
                return -1;
            }

            if (startTime == null)
            {
                startTime = "2000-01-01 01:01:01Z"; //DateTime.ToString("u")
            }

            if (endTime == null)
            {
                endTime = "2200-01-01 01:01:01Z";   //DateTime.ToString("u")
            }

            var start = Convert.ToDateTime(startTime);
            var end = Convert.ToDateTime(endTime);

            var collection = _database.GetCollection<T>(_collectionName);
            var filterBuilder = Builders<T>.Filter;
            var filter = filterBuilder.Gte("CreateTime", start) & filterBuilder.Lte("CreateTime", end);
            try
            {
                var result = collection.DeleteManyAsync(filter).Result;
                if (result == DeleteResult.Unacknowledged.Instance)
                {
                    return -3;
                }
                return 0;
            }
            catch (Exception e)
            {
                return -2;
            }

        }
        /// <summary>
        /// 通过起止时间删除数据
        /// </summary>
        /// <typeparam name="T">类型（必须包含CreateTime字段）</typeparam>
        /// <param name="startTime">起始时间</param>
        /// <param name="endTime">终止时间）</param>
        /// <returns>0-成功</returns>
        public int QueryAllByTime<T>(DateTime startTime, DateTime endTime) where T : class
        {
            if (_initedError || _client == null || _database == null)
            {
                return -1;
            }

            var collection = _database.GetCollection<T>(_collectionName);
            var filterBuilder = Builders<T>.Filter;
            var filter = filterBuilder.Gte("CreateTime", startTime) & filterBuilder.Lte("CreateTime", endTime);
            try
            {
                var result = collection.DeleteManyAsync(filter).Result;
                if (result == DeleteResult.Unacknowledged.Instance)
                {
                    return -3;
                }
                return 0;
            }
            catch (Exception e)
            {
                return -2;
            }

        }
        #endregion

        #region 特定条件查询
        /// <summary>
        /// 对含Result字段的数据进行查询
        /// </summary>
        /// <typeparam name="T">类型（必须包含Result和CreateTime字段）</typeparam>
        /// <param name="startTime">起始时间（可为null）</param>
        /// <param name="endTime">终止时间（可为null）</param>
        /// <param name="result">查询结果</param>
        /// <param name="okCount">OK结果统计（字段值为OK或1）</param>
        /// <param name="ngCount">NG结果统计（字段值为NG或0）</param>
        /// <param name="uph">UPH统计</param>
        /// <returns>0-成功</returns>
        public int QueryManyByTime<T>(string startTime, string endTime, out List<T> result, out long okCount, out long ngCount, out double uph, bool noOk = false, bool noNg = false) where T : class
        {
            if (_initedError || _client == null || _database == null || !PropertyExist<T>("Result"))
            {
                result = null;
                okCount = -1;
                ngCount = -1;
                uph = -1;
                return -1;
            }


            if (startTime == null)
            {
                startTime = "2000-01-01 01:01:01Z"; //DateTime.ToString("u")
            }

            if (endTime == null)
            {
                endTime = "2200-01-01 01:01:01Z";   //DateTime.ToString("u")
            }

            var start = Convert.ToDateTime(startTime);
            var end = Convert.ToDateTime(endTime);

            if (start > end)
            {
                MessageBox.Show("终止日期应该比起始日期晚！", "提示", MessageBoxButtons.OK, MessageBoxIcon.Stop);
                result = null;
                okCount = -1;
                ngCount = -1;
                uph = -1;
                return -10;
            }

            var collection = _database.GetCollection<T>(_collectionName);

            var filterTime = Builders<T>.Filter.Gte("CreateTime", start) & Builders<T>.Filter.Lte("CreateTime", end) & Builders<T>.Filter.Exists("Result", true);

            var subFilters = new List<FilterDefinition<T>>
            {
                filterTime
            };

            var filterNoOk = Builders<T>.Filter.Ne("Result", "OK") & Builders<T>.Filter.Ne("Result", "1");
            var filterNoNg = Builders<T>.Filter.Ne("Result", "NG") & Builders<T>.Filter.Ne("Result", "0");

            if (noOk)
            {
                subFilters.Add(filterNoOk);
            }

            if (noNg)
            {
                subFilters.Add(filterNoNg);
            }

            var filter = Builders<T>.Filter.And(subFilters);

            var sort = Builders<T>.Sort.Ascending("CreateTime");
            var projection = Builders<T>.Projection.Exclude("_id");

            try
            {
                var document = collection.Find<T>(filter).Project<T>(projection).Sort(sort);
                result = document.ToList();
                okCount = result.Count(t => GetModelValue("Result", t).ToUpper() == "OK" || GetModelValue("Result", t).ToUpper() == "1");
                ngCount = result.Count - okCount;

                var uphStartTime = DateTime.Parse(GetModelValue("CreateTime", result.First()));
                var uphEndTime = DateTime.Parse(GetModelValue("CreateTime", result.Last()));
                var time = uphEndTime - uphStartTime;
                if (time.Hours > 8)
                {
                    uph = -1;
                }

                uph = result.Count / time.TotalHours;

                return 0;
            }
            catch (Exception e)
            {
                result = null;
                okCount = -1;
                ngCount = -1;
                uph = -1;
                return -2;
            }
        }
        /// <summary>
        /// 对含Result字段的数据进行查询
        /// </summary>
        /// <typeparam name="T">类型（必须包含Result和CreateTime字段）</typeparam>
        /// <param name="startTime">起始时间（可为null）</param>
        /// <param name="endTime">终止时间（可为null）</param>
        /// <param name="result">查询结果</param>
        /// <param name="okCount">OK结果统计（字段值为OK或1）</param>
        /// <param name="ngCount">NG结果统计（字段值为NG或0）</param>
        /// <param name="uph">UPH统计</param>
        /// <returns>0-成功</returns>
        public int QueryManyByTime(string startTime, string endTime, out string result, out long okCount, out long ngCount, out double uph, bool noOk = false, bool noNg = false)
        {
            if (_initedError || _client == null || _database == null)
            {
                result = null;
                okCount = -1;
                ngCount = -1;
                uph = -1;
                return -1;
            }


            if (startTime == null)
            {
                startTime = "2000-01-01 01:01:01Z"; //DateTime.ToString("u")
            }

            if (endTime == null)
            {
                endTime = "2200-01-01 01:01:01Z";   //DateTime.ToString("u")
            }

            var start = Convert.ToDateTime(startTime);
            var end = Convert.ToDateTime(endTime);

            if (start > end)
            {
                MessageBox.Show("终止日期应该比起始日期晚！", "提示", MessageBoxButtons.OK, MessageBoxIcon.Stop);
                result = null;
                okCount = -1;
                ngCount = -1;
                uph = -1;
                return -10;
            }

            var collection = _database.GetCollection<BsonDocument>(_collectionName);

            var filterTime = Builders<BsonDocument>.Filter.Gte("CreateTime", start) & Builders<BsonDocument>.Filter.Lte("CreateTime", end) & Builders<BsonDocument>.Filter.Exists("Result", true);

            var subFilters = new List<FilterDefinition<BsonDocument>>
            {
                filterTime
            };

            var filterNoOk = Builders<BsonDocument>.Filter.Ne("Result", "OK") & Builders<BsonDocument>.Filter.Ne("Result", "1");
            var filterNoNg = Builders<BsonDocument>.Filter.Ne("Result", "NG") & Builders<BsonDocument>.Filter.Ne("Result", "0");

            if (noOk)
            {
                subFilters.Add(filterNoOk);
            }

            if (noNg)
            {
                subFilters.Add(filterNoNg);
            }

            var filter = Builders<BsonDocument>.Filter.And(subFilters);

            var sort = Builders<BsonDocument>.Sort.Ascending("CreateTime");
            var projection = Builders<BsonDocument>.Projection.Exclude("_id");

            try
            {
                var document = collection.Find<BsonDocument>(filter).Project<BsonDocument>(projection).Sort(sort).ToList();
                result = document.ToJson(JsonWriterSettings.Defaults);
                okCount = result.Count(t => GetModelValue("Result", t).ToUpper() == "OK" || GetModelValue("Result", t).ToUpper() == "1");
                ngCount = document.Count - okCount;

                var uphStartTime = DateTime.Parse(GetModelValue("CreateTime", result.First()));
                var uphEndTime = DateTime.Parse(GetModelValue("CreateTime", result.Last()));
                var time = uphEndTime - uphStartTime;
                if (time.Hours > 8)
                {
                    uph = -1;
                }

                uph = document.Count / time.TotalHours;

                return 0;
            }
            catch (Exception e)
            {
                result = null;
                okCount = -1;
                ngCount = -1;
                uph = -1;
                return -2;
            }
        }
        #endregion
        /// <summary>

        /// 获取类中的属性值
        /// </summary>
        /// <param name="fieldName">属性名</param>
        /// <param name="obj">对象</param>
        /// <returns>属性值</returns>
        private string GetModelValue<T>(string fieldName, T obj)
        {
            try
            {
                var ts = obj.GetType();
                var o = ts.GetProperty(fieldName)?.GetValue(obj, null);
                var value = Convert.ToString(o);
                return string.IsNullOrEmpty(value) ? null : value;
            }
            catch
            {
                return null;
            }
        }

        /// <summary>
        /// 设置类中的属性值
        /// </summary>
        /// <param name="fieldName"></param>
        /// <param name="obj"></param>
        /// <returns>成功-true</returns>
        private bool SetModelValue<T>(string fieldName, string value, T obj)
        {
            try
            {
                //var ts = obj.GetType();
                //var v = Convert.ChangeType(value, ts.GetProperty(fieldName)?.PropertyType ?? throw new InvalidOperationException());
                //ts.GetProperty(fieldName)?.SetValue(obj, v, null);
                return true;
            }
            catch
            {
                return false;
            }
        }
        /// <summary>
        /// 判断类中属性是否存在
        /// </summary>
        /// <typeparam name="T">类型</typeparam>
        /// <param name="fieldName">属性名</param>
        /// <returns>存在—true</returns>
        private bool PropertyExist<T>(string fieldName)
        {
            try
            {
                var ts = typeof(T);
                var o = ts.GetProperty(fieldName);
                return o != null;
            }
            catch (Exception e)
            {
                return false;
            }
        }
    }
}
