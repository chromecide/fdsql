if (typeof define !== 'function') {
    var define = require('amdefine')(module);
}

define(['mysql'], function(mysql){
    var mixin = {
        //called when first mixing in the functionality
        init: function(cfg, callback){

            var self = this;
            var errs = false;
            
            self.requireMixin('FluxData/data/store', cfg, function(){
                //override default functions
                self._fetch = self.fetch;
                self.fetch = self.mysql_fetch;
                self._sync = self.sync;
                self.sync = self.mysql_sync;

                if(!self.get('database')){
                    self.emit('error', new Error('No database supplied'));
                    return;
                }

                if(!self.get('host')){
                    self.set('host', 'localhost');
                }

                if(!self.get('user')){
                    self.set('user', 'root');
                }

                var mysqlConnection = mysql.createConnection({
                  host     : self.get('host'),
                  user     : self.get('user'),
                  password : self.get('password'),
                  database : self.get('database')
                });


                self.set('mysql', {
                    connection: mysqlConnection
                });

                mysqlConnection.connect(function(err){
                    if(err){
                        self.emit('error', err);
                        return;
                    }else{
                        if(callback){
                            callback(errs, self);
                        }
                    }
                });
                //console.log(self.sync.toString());
                mysqlConnection.on('error', function(err){
                    self.emit('error', err);
                });
            });
        },
        //called when something is published to this channel
        publish: function(topic, data){
            var self = this;
        },
        mysql_fetch: function(callback){
            var self = this;
            self.mysql.loadTablesAsModels.call(self, function(err, modelList){
                for(var i=0;i<modelList.length;i++){
                    self.set('models.'+modelList[i].get('name'), modelList[i]);
                }

                var colIndex = 0;
                //TODO: need to create the collection objects
                function collectionLoop(cb){
                    if(colIndex===modelList.length){
                        if(cb){

                            cb();
                        }
                        return;
                    }

                    var newCollection = new self.constructor({
                        name: modelList[colIndex].get('name'),
                        mixins:[
                            {
                                type: 'FluxData/data/collection',
                                model: modelList[colIndex],
                                store: self
                            }
                        ]
                    });
                    newCollection.once('channel.ready', function(){

                        self.set('collections.'+newCollection.get('name'), newCollection);
                        colIndex++;
                        collectionLoop(cb);
                    });
                }
                collectionLoop(function(){
                    if(callback){
                        callback(false, self.get('models'), self.get('collections'));
                        return;
                    }
                });
            });
        },
        mysql_sync: function(commit, callback){
            var self = this;
            if((typeof commit)=='function'){
                callback = commit;
                commit = true;
            }
            self.mysql.loadTableNames.call(self, function(err, currentTableNames){
                console.log(currentTableNames);
                var models = self.get('models');
                var tablesToAddOrUpdate = [];
                var tablesToRemove = [];
                var i;
                for(i=0;i<currentTableNames.length;i++){
                    console.log('CHECKING:', currentTableNames[i]);
                    var model = models[currentTableNames[i]];
                    if(model){

                        tablesToAddOrUpdate.push(currentTableNames[i]);
                    }else{
                        tablesToRemove.push(currentTableNames[i]);
                    }
                }

                for(var modelName in models){
                    var modelFound = false;
                    for(i=0;i<currentTableNames.length;i++){
                        if(modelName==currentTableNames[i]){
                            modelFound = true;
                        }
                    }
                    if(!modelFound){
                        tablesToAddOrUpdate.push(models[modelName].get('name'));
                    }
                }

                var statements = [];
                function removeTableLoop(cb){
                    var tableName = tablesToRemove.shift();
                    if(!tableName){
                        if(cb){
                            cb();
                        }
                        return;
                    }

                    var sql = 'DROP TABLE '+tableName+';';
                    statements.push(sql);
                    removeTableLoop(cb);
                }

                function addUpdateTableLoop(cb){
                    var tableName = tablesToAddOrUpdate.shift();
                    if(!tableName){
                        if(cb){
                            cb();
                        }
                        return;
                    }

                    self.mysql.modelToTableSql.call(self, self.get('models.'+tableName), function(err, sql){
                        statements.push(sql);
                        addUpdateTableLoop(cb);
                    });
                }

                removeTableLoop(function(){
                    addUpdateTableLoop(function(){

                        var connection = self.get('mysql.connection');
                        if(commit===true){
                            console.log(statements.join(' '));
                            for(var i=0;i<statements.length;i++){
                                connection.query(statements[i], function(err, result){
                                    console.log(arguments);
                                    console.log(result);
                                    /*if(callback){
                                        callback(err, result);
                                    }*/
                                });    
                            }
                            
                        }else{
                            if(callback){
                                callback(false, statements);
                            }
                        }
                    });
                });
            });
        },
        mysql:{
            loadTablesAsModels: function(callback){
                var self = this;
                self.mysql.loadTableNames.call(self, function(err, tableNames){
                    var modelList = [];
                    for(var i=0;i<tableNames.length;i++){
                        self.mysql.tableToModel.call(self, tableNames[i], function(err, newModel){
                            if(newModel){
                                //self.addModel(newModel);
                                modelList.push(newModel);
                            }

                            if(modelList.length==tableNames.length){
                                if(callback){
                                    callback(false, modelList);
                                }
                            }
                        });
                    }
                });
            },
            loadTableNames: function(callback){
                var self = this;
                var sql = "SHOW TABLES;";

                var conn = self.get('mysql.connection');
                conn.query(sql, function(err, rows, fields){
                    var retArr = [];
                    var fieldName = fields[0].name;
                    for(var i=0;i<rows.length;i++){
                        retArr.push(rows[i][fieldName]);
                    }
                    if(callback){
                        callback(false, retArr);
                    }
                });
            },
            loadTableFields: function(tableName, callback){
                var self = this;

                var sql = "DESCRIBE ??;";

                var conn = self.get('mysql.connection');

                sql = conn.query(sql, [tableName], function(err, rows, fields){
                    
                    if(callback){
                        callback(false, tableName, rows);
                    }
                });
            },
            tableToModel: function(tableName, callback){
                var self = this;
                var mixinCfg = {
                    type: 'FluxData/data/model',
                    fields: []
                };

                self.mysql.loadTableFields.call(self, tableName, function(err, tName, fields){
                    if(err){
                        self.emit('error', err);
                        return;
                    }
                    if(fields){
                        for(var i=0;i<fields.length;i++){
                            var field = fields[i];
                            
                            var fieldCfg = self.mysql.typeToField.call(self, fields[i]);
                            mixinCfg.fields.push(fieldCfg);
                        }
                        
                        var modelCfg = {
                            name: tableName,
                            mixins: [
                                mixinCfg
                            ]
                        };

                        var newModel = new self.constructor(modelCfg);
                        newModel.once('channel.ready', function(){
                            if(callback){
                                callback(false, newModel);
                            }
                        });
                    }else{
                        callback(false, undefined);
                    }
                });
            },
            typeToField: function(field){
                //console.log('PROCESSING FIELD', field.Field);
                var self = this;

                var returnType = {
                    name: field.Field,
                    required: field.Null=='NO'?true:false,
                    default: field.Default,
                    index: field.Key=='PRI'?true:false
                };
                
                if(field.Key=='PRI'){
                    if(field.Extra=='auto_increment'){
                        returnType.index = 'auto_increment';
                    }
                }

                var typeParts = field.Type.replace(')','').split('(');

                switch(typeParts[0].toLowerCase()){
                    case 'varchar':
                        returnType.datatype = 'string';
                        returnType.maxlength = typeParts[1]*1;
                        break;
                    case 'int':
                        returnType.datatype = 'number';
                        returnType.precision = typeParts[1]*1;
                        returnType.decimalplaces = 0;
                        break;
                    case 'decimal':
                        returnType.datatype = 'number';
                        precisionparts = typeParts[1].split(',');
                        returnType.precision = precisionparts[0]*1;
                        returnType.decimalplaces=precisionparts[1]*1;
                        break;
                    case 'tinyint':
                        if(typeParts[1]=='1'){
                            returnType.datatype = 'boolean';
                        }
                        break;
                    case 'datetime':
                        returnType.datatype = 'date';
                        break;
                    case 'text':
                        returnType.datatype = 'string';
                        returnType.maxlength=false;
                        break;
                    case 'timestamp':
                        returnType.datatype = 'timestamp';
                        break;
                    default:
                        console.log('NO TYPE-TO-FIELD for', field.Type);
                        break;
                }
                return returnType;
            },
            fieldToType: function(field){
                var self = this;

                var colType = '';

                switch(field.get('datatype').name.toLowerCase()){
                    case 'string':
                        if(!field.get('maxlength')){
                            colType = 'text';
                        }else{
                            colType = 'varchar('+field.get('maxlength')+')';
                        }
                        break;
                    case 'number':
                        if(field.get('decimalplaces')>0){
                            colType = 'decimal('+(field.get('precision') || 10)+', '+field.get('decimalplaces');
                        }else{
                            colType = 'int('+(field.get('precision') || 11)+')';
                        }
                        break;
                    case 'timestamp':
                        colType = 'timestamp';
                        break;
                    case 'boolean':
                        colType = 'tinyint(1)';
                        break;
                }

                if(field.get('required')===true){
                    colType+=' NOT NULL';
                }

                if(field.get('default')!==undefined && field.get('default')!==null){
                    switch(field.get('datatype').name.toLowerCase()){
                        case 'string':
                            colType+= ' default '+(field.get('default')=='NULL'?'NULL': "'"+field.get('default')+"'");
                            break;
                        default:
                            colType+= ' default '+field.get('default');
                            break;
                    }
                }

                if(field.get('index')){
                    if(field.get('index')==='auto_increment'){
                        colType+=' AUTO_INCREMENT';
                    }

                    colType+=' PRIMARY KEY';
                }

                return colType;
            },
            modelToTableSql: function(model, callback){
                var self = this;

                var columns = [];

                //load a copy of the model directly from the database
                self.mysql.tableToModel.call(self, model.get('name'), function(err, currentModel){
                    var fields = model.get('fields');
                    var qry;
                    if(currentModel){
                        var currentFields = currentModel.get('fields');
                        var fieldsToAdd = [];
                        var fieldsToUpdate = [];
                        var fieldsToRemove = [];

                        var fieldName,
                            currentFieldName;
                        
                        //create new fields first
                        for(fieldName in fields){
                            var newItemFound = false;
                            for(currentFieldName in currentFields){
                                if(fieldName==currentFieldName){
                                    newItemFound = true;
                                }
                            }
                            
                            if(!newItemFound){
                                fieldsToAdd.push(fieldName);
                            }
                        }

                        //update existing fields
                        for(fieldName in fields){
                            var field = fields[fieldName];
                            var currentField = currentFields[fieldName];
                            
                            if(currentField){
                                //Do the compare
                                var fieldData = field.get();
                                for(var key in fieldData){
                                    if(key!='id'){
                                        if(field.get(key)!=currentField.get(key)){

                                            fieldsToUpdate.push(fieldName);
                                        }
                                    }
                                }
                            }
                        }

                        //remove fields
                        
                        for(currentFieldName in currentFields){
                            var currentItemFound = false;
                            for(fieldName in fields){
                                if(fieldName==currentFieldName){
                                    currentItemFound = true;
                                }
                            }
                            if(!currentItemFound){
                                fieldsToRemove.push(currentFieldName);
                            }
                        }

                        var colType, statements=[];
                        var field;
                        for(i=0;i<fieldsToAdd.length;i++){
                            field = model.get('fields.'+fieldsToAdd[i]);
                            
                            colType = 'ADD '+field.get('name')+' '+self.mysql.fieldToType.call(self, field);
                            statements.push(colType);
                        }

                        for(i=0;i<fieldsToUpdate.length;i++){
                            field = model.get('fields.'+fieldsToUpdate[i]);
                            colType = 'CHANGE '+field.get('name')+' '+field.get('name')+' '+self.mysql.fieldToType.call(self, field);
                            statements.push(colType);
                        }

                        for(i=0;i<fieldsToRemove.length;i++){
                            field = model.get('fields.'+fieldsToRemove[i]);
                            colType = 'DROP '+fieldsToRemove[i];
                            statements.push(colType);
                        }

                        qry = 'ALTER TABLE '+model.get('name')+' '+statements.join(',')+';';

                    }else{
                        
                        for(var fieldName in fields){
                            var field = fields[fieldName];
                            
                            var colType = self.mysql.fieldToType.call(self, field);
                            columns.push((fieldName+' '+colType).toString());

                        }
                        
                        var connection = self.get('mysql.connection');

                        qry = 'CREATE TABLE IF NOT EXISTS '+model.get('name')+' ('+columns.join(',')+');';//connection.escapeId(sql, [model.get('name'), columns]);
                                
                    }
                    if(callback){
                        callback(false, qry);
                    }
                });
                
            }
        }
    };
    
    return mixin;
});