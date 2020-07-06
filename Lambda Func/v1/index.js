const config = require('./config.json');
const snowflake = require('./snowflakeWrapper.js');
var dbConn = null;

exports.handler = async (event) => {
    return snowflake.connect()
    .then((dbConnection)=>{
        dbConn = dbConnection;
        return;
    
    }).then(()=>{
        var SQL = 'insert into \"HEALTHKIT\".\"PUBLIC\".\"HEALTHKIT_IMPORT\"(select parse_json (column1) from values(\'' + JSON.stringify(event.body) + '\'))';
        console.log(SQL);
        
        return snowflake.runSQL(dbConn, SQL).then((data)=>{
            console.log(Date.now(), data);

            const response = {
                statusCode: 200,
                body: data,
            };
            return response;

        })
    })

    
};











