package com.xxl.job.core.handler.impl;

import com.xxl.job.core.biz.model.ReturnT;
import com.xxl.job.core.executor.impl.XxlJobSpringExecutor;
import com.xxl.job.core.handler.IJobHandler;
import com.xxl.job.core.log.XxlJobLogger;
import org.springframework.jdbc.datasource.DataSourceUtils;
import org.springframework.util.StringUtils;

import javax.sql.DataSource;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.Types;
import java.util.ArrayList;


public class ProcedureJobHandler extends IJobHandler {

    private String procedureName;

    private String dataSourceName;

    public ProcedureJobHandler(String procedureName) {
        this(null,procedureName);
    }

    public ProcedureJobHandler(String dataSourceName, String procedureName) {
        this.procedureName = procedureName;
        this.dataSourceName = dataSourceName;
    }

    public String getIdentify(){
        return dataSourceName == null? procedureName : (dataSourceName +"."+ procedureName);
    }


    public String getProcedureName() {
        return procedureName;
    }

    public String getDataSourceName() {
        return dataSourceName;
    }

    @Override
    public ReturnT<String> execute(String param) throws Exception {

        XxlJobLogger.log("数据源:"+dataSourceName+"存储过程:"+ procedureName +" 参数:"+param+" -----------");

        DataSource dataSource = null;
        try{
            if(StringUtils.hasText(dataSourceName)){
                dataSource = XxlJobSpringExecutor.getApplicationContext().getBean(dataSourceName, DataSource.class);
            }else{
                dataSource = XxlJobSpringExecutor.getApplicationContext().getBean(DataSource.class);
            }
        }catch (Exception e){
            XxlJobLogger.log("获取数据源出错:"+ e.getMessage());
        }


        if(dataSource == null){
            XxlJobLogger.log("获取数据源出错:"+ dataSourceName);
            return ReturnT.FAIL;
        }

        Connection connection = null;
        try{
            //1.拼接参数
            ArrayList<String> params = new ArrayList<>();
            if(StringUtils.hasText(param)){
                String[]paramArray =  param.split(",");
                for(int i = 0; i < paramArray.length;i++){
                    params.add(paramArray[i]);
                }
            }
            StringBuffer sqlStr = new StringBuffer();
            sqlStr.append("{call ").append(procedureName).append("(?,?");
            for(int i = 0; i < params.size();i++){
                sqlStr.append(",?");
            }
            sqlStr.append(")}");

            XxlJobLogger.log("调用sql:"+ sqlStr.toString());

            //2.获取数据流链接
            connection = DataSourceUtils.getConnection(dataSource);

            CallableStatement callStmt = connection.prepareCall(sqlStr.toString());

            // 对象来将参数化的 SQL 语句发送到数据库
            // 根据参数组的值,设置存储过程的值
            callStmt.registerOutParameter(1, Types.INTEGER);
            callStmt.registerOutParameter(2, Types.VARCHAR);
            for (int i = 0; i < params.size(); i++) {
                callStmt.setString(i + 3, params.get(i));
            }

            callStmt.execute();

            int outCode = callStmt.getInt(1);
            String outMsg = callStmt.getString(2);

            XxlJobLogger.log("存储过程调用返回代码:"+ outCode + "信息:" + outMsg);
            if(outCode != 0){
                return new ReturnT<>(ReturnT.FAIL_CODE,outMsg);
            }
        }catch (Exception e){
            XxlJobLogger.log("存储过程调用出错:"+ e.getMessage());
            return new ReturnT<>(ReturnT.FAIL_CODE,"存储过程调用出错");
        }finally {
            if(connection != null){
                DataSourceUtils.releaseConnection(connection,dataSource);
            }
        }
        return ReturnT.SUCCESS;
    }
}
