/// <reference path="../../../../../applibs/sdk/jQuery-2.1.3.min.js" />
/// <reference path="../../../../../applibs/sdk/json.js" />
/// <reference path="../../../../../applibs/sdk/date.js" />
/// <reference path="../../../../../applibs/sdk/baiduTpls.js" /> 
/// <reference path="../../../../../applibs/sdk/hhls.js" /> 
/// <reference path="../../../../../applibs/sdk/hhls_wxConfirm.js" />
/// <reference path="../../../../../applibs/bootstrap-3.3.5-dist/js/bootstrap.min.js" /> 
/// <reference path="../../box/box.js" />
/// <reference path="../../../../../applibs/bootstrap-3.3.5-dist/datetimepicker/js/bootstrap-datetimepicker.js" /> 

var Explanation = {
    Datas: {
        Memos: [
              { bimg: "14_14", simg: "14", caption: "Create Pipeline", say: "Create" },
              { bimg: "1_1", simg: "1", caption: "Upload Workload", say: "Upload" },
              { bimg: "6_6", simg: "6", caption: "Start Evaluation", say: "Start" },
              { bimg: "4_4", simg: "4", caption: "Accept Optimization", say: "Accept" },
        ],
    },
    Tpls: {
        tplPage: { P: "Modules/Explanation/tplPage.htm", C: "" },  
    },
    Load: function () {
        var me = Explanation;
        try {
            hhls.GetTpls(me.Tpls, function () {
                me.Refresh();
            });
        }
        catch (e) {; }
    },
    Refresh: function () {
        var me = Explanation;
        try { 
            var aHtml = bt(me.Tpls.tplPage.C, { tplData: me.Datas.Memos });
            hhls.fillElement(".divPage", aHtml);
            $('.divItem .ImgIcon a').lightBox();
            // update intWidth
            // $('#lightbox-container-image-data-box').css({ width: intWidth });
        }
        catch (E) {; }
    },

};
