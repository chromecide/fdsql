if (typeof define !== 'function') {
    var define = require('amdefine')(module);
}

define(['./mixins/mysql/store'], function(mysqlStore){
    
    var mixin = {
        mysql: {
            Store: mysqlStore
        }
    };
    
    return mixin;
});