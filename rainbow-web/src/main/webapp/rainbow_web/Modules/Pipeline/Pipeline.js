/// <reference path="../../../../../applibs/sdk/jQuery-2.1.3.min.js" />
/// <reference path="../../../../../applibs/sdk/json.js" />
/// <reference path="../../../../../applibs/sdk/date.js" />
/// <reference path="../../../../../applibs/sdk/baiduTpls.js" />
/// <reference path="../../../../../applibs/sdk/base64.js" />
/// <reference path="../../../../../applibs/sdk/hhls.js" />
/// <reference path="../../../../../applibs/sdk/hhac.js" />
/// <reference path="../../../../../applibs/sdk/hhls_wxConfirm.js" />
/// <reference path="../../../../../applibs/bootstrap-3.3.5-dist/js/bootstrap.min.js" /> 
/// <reference path="../Index/Index.js" />
/// <reference path="../../../../../applibs/bootstrap-3.3.5-dist/datetimepicker/js/bootstrap-datetimepicker.js" /> 

// setTimeout
var tLists;

var Pipeline = {
    Datas: {
        Pipelines: [
            //{ no: "001", caption: "Pipeline001", format: "", description: "1213421" },
            //{ no: "002", caption: "Pipeline002", format: "Orc", description: "12342" },
            //{ no: "003", caption: "Pipeline003", format: "Carbondata", description: "35234" },
            //{ no: "004", caption: "Pipeline004", format: "", description: "4455345" },
        ],
        PostID: 0,
        Optimizer: [0, 0, 0],
    },
    Tpls: {
        tplPage: { P: "Modules/Pipeline/tplPage.html", C: "" },
        tplTableItem: { P: "Modules/Pipeline/tplTableItem.html", C: "" },
        tplDtl: { P: "Modules/Pipeline/tplDtl.html", C: "" },
    },
    Load: function () {
        var me = Pipeline;
        try {
            hhls.GetTpls(me.Tpls, function () {
                me.Refresh();
            });
        }
        catch (e) {; }
    },
    Refresh: function () {
        var me = Pipeline;
        try {
            var aHtml = me.Tpls.tplPage.C;
            hhls.fillElement(".divPage", aHtml);
            me.RefreshTable();
            me.getDataSource(); 
            tLists = setInterval(function () {
                Pipeline.RefreshTable();
            }, 5 * 1000);
        }
        catch (E) {; }
    },
    RefreshTable: function () {
        var me = Pipeline;
        try {
            $.get(Index.Action.getPipelineData, {}, function (data) {
                me.Datas.Pipelines = hhls.getJsonObj(data);
                var aHtml = bt(me.Tpls.tplTableItem.C, { tplData: me.Datas.Pipelines });
                hhls.fillElement("#tBodyPipelines", aHtml);
            });
        }
        catch (E) {; }
    },
    getDataSource: function () {
        var me = Pipeline;
        try {
            $.get(Index.Action.getDataSource, {}, function (data) {
                var arr = data.split(',');
                var aHtml = "";
                for (var i in arr) {
                    aHtml += '<option value="' + arr[i] + '">' + arr[i] + '</option>';
                }
                hhls.fillElement("#txtSource", aHtml);
            });
        }
        catch (E) {; }
    },
    doShowDlg: function (aIndex) {
        var me = Pipeline;
        try {
            me.Datas.PostID = aIndex == 0 ? 0 : me.Datas.Pipelines[aIndex].no;
            var aID = "dlgPipeline";
            var onShow = function (e) {
                if (me.Datas.PostID != 0) {
                    var aInfo = me.Datas.Pipelines[aIndex];
                    $("#txtCaption").val(aInfo.caption);
                    $("#txtURL").val(aInfo.url);
                    $("#txtFormat").val(aInfo.format);
                    $("#txtDesc").val(aInfo.description);
                } else {
                    var no = hhls.getGuid();
                    no = "44ba30d7abdbe13ab2c886f18c0f5555";
                    $("#txtNo").val(no);
                    //var store = $("#txtStore").val() + no.substr(0, 5) + "/";
                    var store = "/rainbow-web/common/" + no.substr(0, 5) + "/";
                    $("#txtStore").val(store);
                }
            };
            var onHide = function (e) {
                //hhls.removeElement("#" + aID);
                //me.RefreshTable();
            };
            var aDlg = $("#" + aID).unbind("hidden.bs.modal").bind("hidden.bs.modal", onHide).unbind("shown.bs.modal").bind("shown.bs.modal", onShow);
            aDlg.modal("show");
        }
        catch (E) {; }
    },
    doStop: function (aIndex) {
        var me = Pipeline;
        try {
            var aPs = { pno: me.Datas.Pipelines[aIndex].no };
            $.post(Index.Action.doStop, aPs, function (data, status) {
                me.RefreshTable();
            });
        }
        catch (e) {; }
    },
    doDelete: function (aIndex) {
        var me = Pipeline;
        try {
            var aFlag = window.confirm("Are you sure to remove ?");
            if (aFlag) {
                var aPs = { pno: me.Datas.Pipelines[aIndex].no }; 
                $.post(Index.Action.doDelete, aPs, function (data, status) {
                    me.RefreshTable();
                }); 
            }
        }
        catch (e) {; }
    },
    //doPickItem: function (aIndex) {
    //    var me = Pipeline;
    //    try {
    //        var format = $("#txtFormat").val();
    //        var aMenu = $(".divSpan");
    //        if (me.Datas.Optimizer[aIndex] == 0) {
    //            me.Datas.Optimizer[aIndex] = 1;
    //            $(aMenu[aIndex]).removeClass("btn-default");
    //            $(aMenu[aIndex]).addClass("btn-success");
    //            if (aIndex == 1) {
    //                var aHtml = ' ';
    //                $($(".divRowItem")[2]).after(aHtml);
    //                if (format == "Orc") {
    //                    $("#txtSize").val(64);
    //                } else if (format == "Parquet") {
    //                    $("#txtSize").val(128);
    //                }
    //            } else if (aIndex == 2) {
    //                var aHtml = '';
    //                $($(".divRowItem")[2]).after(aHtml);
    //                if (format == "Orc") {
    //                    $("#txtCodec").val("Zlib");
    //                } else if (format == "Parquet") {
    //                    $("#txtCodec").val("");
    //                }
    //            }
    //        } else {
    //            me.Datas.Optimizer[aIndex] = 0;
    //            $(aMenu[aIndex]).removeClass("btn-success");
    //            $(aMenu[aIndex]).addClass("btn-default");
    //            if (aIndex == 1) {
    //                $("#rowgroupsize").remove();
    //            } else if (aIndex == 2) {
    //                $("#compression").remove();
    //            }
    //        }
    //    }
    //    catch (e) {; }
    //},
    doShowDtl: function (aIndex) {
        var me = Pipeline;
        try {
            var aInfo = me.Datas.Pipelines[aIndex];
            var aHtml = bt(me.Tpls.tplDtl.C, { tplData: aInfo });
            hhls.fillElement(".divPage", aHtml);
            $("#curPipelineCaption").text(aInfo.caption);
        }
        catch (e) {; }
    },
    doChangeFormat: function () {
        var me = Pipeline;
        try {
            var aFormat = $("#txtFormat").val();
            if (aFormat == "Parquet") {
                $("#txtSize").val(128);
                $("#txtCodec").val("NONE");
            } else {
                $("#txtSize").val(64);
                $("#txtCodec").val("ZLIB");
            }
        }
        catch (e) {; }
    },
    doChangeDataSource: function () {
        var me = Pipeline;
        try {
            var aDataSource = $("#txtSource").val();
            if (aDataSource == "hdfs") {
                $("#txtURL").val();
                $("#txtStore").val();
            } else {
                $("#txtSource").val("hdfs");
                //alert("The system does't support kafka-source, you can add the function with the interface we provide.");
                Init.Web_Toast("The system does't support kafka-source, you can add the function with the interface we provide.", 1);
            }
        }
        catch (e) {; }
    },

};


$('#uploaderForm').on('submit', function (e) {
    var me = Pipeline;
    // must be show in the main page
    e.preventDefault();
    var formData = new FormData(this);
    if (formData.get("schema").name == "") {
        //alert("Please select corresponding schema file!");
        Init.Web_Toast("Please select corresponding schema file!", 1);
        return;
    }
    //var No = $("#txtNo").val();
    //formData.append("No", No);
    //var optimizerType = ["columncorder", "rowgroupsize", "compression"]; 
    //formData.append("columncorder", me.Datas.Optimizer[0] == 1 ? 1 : 0);
    //formData.append("rowgroupsize", me.Datas.Optimizer[1] == 1 ? $("#txtSize").val() : 0);
    //formData.append("compression", me.Datas.Optimizer[2] == 1 ? $("#txtCodec").val() : "");

    $.ajax({
        url: $(this).attr('action'),
        type: $(this).attr('method'),
        data: formData,
        processData: false,
        contentType: false,
        success: function (data) {
            $('#dlgPipeline').modal('toggle');
            Pipeline.RefreshTable() 
            //alert("Uploaded succeed.");
            Init.Web_Toast("Uploaded succeed.", 1);
        },
        error: function (jXHR, textStatus, errorThrown) {
            $('#dlgPipeline').modal('toggle');
            //alert("Error: Upload failed. " + errorThrown);
            Init.Web_Toast("Error: Upload failed. " + errorThrown, 1);
        }
    });
});