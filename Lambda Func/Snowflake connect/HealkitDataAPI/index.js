const snowflake = require('./snowflakeWrapper.js');
var dbConn = null;

exports.handler = async (event) => {
    return snowflake.connect()
    .then((dbConnection)=>{
        dbConn = dbConnection;
        return;
    
    }).then(()=>{
        console.log(event.body);
        var SQL = event.body.select;
        console.log(SQL);
        
        return snowflake.runSQL(dbConn, SQL).then((data)=>{
            console.log(Date.now(), data);

            const response = {
                statusCode: 200,
                body: data,
            };
            return response;

        }) //end run sql

    })

    
};










