/// <reference path="../../../../../applibs/sdk/jQuery-2.1.3.min.js" />
/// <reference path="../../../../../applibs/sdk/json.js" />
/// <reference path="../../../../../applibs/sdk/date.js" />
/// <reference path="../../../../../applibs/sdk/baiduTpls.js" />
/// <reference path="../../../../../applibs/sdk/base64.js" />
/// <reference path="../../../../../applibs/sdk/hhls.js" />
/// <reference path="../../../../../applibs/sdk/hhac.js" />
/// <reference path="../../../../../applibs/sdk/hhls_wxConfirm.js" />
/// <reference path="../../../../../applibs/bootstrap-3.3.5-dist/js/bootstrap.min.js" /> 
/// <reference path="../Optimization/Optimization.js" />
/// <reference path="../../../../../applibs/bootstrap-3.3.5-dist/datetimepicker/js/bootstrap-datetimepicker.js" /> 

var Index = {
    Datas: {
        ModuleIndex: 0,
        Items: [
            //{ Key: "Explanation", Caption: "运行解释", Icon: "" },
            { Key: "Pipeline", Caption: "配置管理", Icon: "" },
            { Key: "Optimization", Caption: "查询信息收集", Icon: "" }
        ],

    },
    Action: {
        // Pipeline List
        getPipelineData: "http://127.0.0.1:8080/rw/getPipelineData",
        getDataSource: "http://127.0.0.1:8080/rw/getDataUrl",
        doStop: "http://127.0.0.1:8080/rw/stop",
        doDelete: "http://127.0.0.1:8080/rw/delete",
        // Sampling
        doSampling: "http://127.0.0.1:8080/rw/startSampling",
        // Workload Upload
        queryUpload: "http://127.0.0.1:8080/rw/queryUpload",
        // Layout Strategy
        accept: "http://127.0.0.1:8080/rw/accept",
        getLayout: "http://127.0.0.1:8080/rw/getLayout",
        getOrderedLayout: "http://127.0.0.1:8080/rw/getOrderedLayout",
        getCurrentLayout: "http://127.0.0.1:8080/rw/getCurrentLayout",
        optimization: "http://127.0.0.1:8080/rw/optimization",
        getEstimate_Sta: "http://127.0.0.1:8080/rw/getEstimate_Sta",
        getOrdered: "http://127.0.0.1:8080/rw/getOrdered",
        // Evaluation
        startEvaluation: "http://127.0.0.1:8080/rw/startEvaluation",
        getStatistic: "http://127.0.0.1:8080/rw/getStatistic",
        getQuery: "http://127.0.0.1:8080/rw/getQuery",
        // Pipeline Process Timeline
        getProcessState: "http://127.0.0.1:8080/rw/getProcessState",
    },
    Tpls: {
    },
    Load: function () {
        var me = Index;
        try {
            me.Refresh();
        }
        catch (e) {; }
    },
    Refresh: function () {
        var me = Index;
        try {

            clearTimeout(tLists);
            clearTimeout(tEstimation);
            clearTimeout(tEvaluation);
            clearTimeout(tProsess);
            clearTimeout(tLayout);
            clearTimeout(tSampling);

            var aItems = $(".menus ul li");
            aItems.removeClass("active");
            $(aItems[me.Datas.ModuleIndex]).addClass("active");
            hhls.clearElement("#divBody");
            var aFun = me.Datas.Items[me.Datas.ModuleIndex].Key;
            aFun += ".Load();";
            try {
                eval(aFun);
            }
            catch (Ex1) {; }
        }
        catch (E) {; }
    },
    OnClickMenuItem: function (aIndex) {
        var me = Index;
        try {
            me.Datas.ModuleIndex = aIndex;
            me.Refresh();
        }
        catch (E) {; }
    },

};



var Init = {
    Datas: {
    },
    Path: {
    },
    //Toast Style
    Utility: {
        WxToast: "<div id=\"wxToast\">"
                    + "<div class=\"wx_transparent\"></div>"
                    + "<div class=\"wx-toast\">"
                        + "<div class=\"sk-spinner sk-spinner-three-bounce\">"
                            + "<div class=\"sk-bounce1\"></div>"
                            + "<div class=\"sk-bounce2\"></div>"
                            + "<div class=\"sk-bounce3\"></div>"
                        + "</div>"
                        + "<p class=\"wx-toast_content\">data loading</p>"
                    + "</div>"
                + "</div>",
        WebToast: "<div id=\"webToast\">"
                    + "<div class=\"web_transparent\"></div>"
                    + "<div class=\"web-toast\">"
                        + "<div class=\"sk-spinner sk-spinner-three-bounce\">"
                            + "<div class=\"sk-bounce1\"></div>"
                            + "<div class=\"sk-bounce2\"></div>"
                            + "<div class=\"sk-bounce3\"></div>"
                        + "</div>"
                        + "<p class=\"web-toast_content\">data loading</p>"
                    + "</div>"
                + "</div>",
        Loading: "<div class='ibox'><div class='ibox-content'><div class='sk-spinner sk-spinner-three-bounce'><div class='sk-bounce1'></div><div class='sk-bounce2'></div><div class='sk-bounce3'></div></div></div></div>",
    },
    //web Toast
    WebToast: function (aContent) {
        var me = Init;
        try {
            $("body").append(me.Utility.WebToast);
            var w = $(window).width();
            var aW = $(".web-toast").width();
            var left = (w - aW) / 2;
            $(".web-toast").css("left", left + "px");
            if (aContent != "")
                $(".web-toast_content").text(aContent);
        }
        catch (e) {; }
    },
    WxToast: function (aContent) {
        var me = Init;
        try {
            $("body").append(me.Utility.WxToast);
            var w = $(window).width();
            var aW = $(".wx-toast").width();
            var left = (w - aW) / 2;
            $(".wx-toast").css("left", left + "px");
            if (aContent != "")
                $(".wx-toast_content").text(aContent);
        }
        catch (e) {; }
    },
    //Toast
    Web_Toast: function (aContent, aTimeOut) {
        var me = Init;
        try {
            me.WebToast(aContent);
            me.ClearToast("#webToast", aTimeOut);
        }
        catch (e) {; }
    },
    Wx_Toast: function (aContent, aTimeOut) {
        var me = Init;
        try {
            me.WxToast(aContent);
            me.ClearToast("#wxToast", aTimeOut);
        }
        catch (e) {; }
    },
    //clear Toast, set time
    ClearToast: function (aElement, aTimeOut) {
        var me = Init;
        try {
            setTimeout(function () {
                $(aElement).remove();
            }, aTimeOut * 1000);
        }
        catch (e) {; }
    },
    //load Pciture
    LoadWxImg: function () {
        var me = Init;
        try {
            var aImgs = $(".WxImg");
            $.each(aImgs, function (aInd, aItem) {
                try {
                    var aImg = $(aItem);
                    var aKey = aImg.attr("Key");
                    if (aKey.length > 0) {
                        var aUrl = me.Datas.MediaPath + Common.Config.DataSvc.WxSrc + "/" + aKey + ".jpg";
                        aImg.attr("src", aUrl);
                    }
                }
                catch (ee) {; }
            });
        }
        catch (e) {; }
    },
    CompareDate: function (d1, d2) {
        var m1 = new Date(d1.replace(/-/g, "\/"));
        var m2 = new Date(d2.replace(/-/g, "\/"));
        return m1 < m2;
    }

}