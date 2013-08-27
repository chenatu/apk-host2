/*jslint browser: true, devel: true, indent: 4, nomen:true, vars: true */
/*global define */

define(function (require, exports, module) {
    "use strict";

    var BaseForm = require('./BaseForm');
    var $ = require('../lib/jquery');

    var BatchedUploadForm = BaseForm.extend({
        events: {
            'click .btn-submit2': 'onSubmit'
        },

        url: "/batchedupload",

        validateForm: function () {
            BaseForm.prototype.validateForm.apply(this, arguments);
        
            var apkDir = $('input[name="apkDir"]').val();
            if (!apkDir) {
                alert('请输入apk文件夹');
                return false;
            }
            return true;
        },

        onSuccess: function () {
            alert("上传成功");
        }
        
    });

    return BatchedUploadForm;
});
