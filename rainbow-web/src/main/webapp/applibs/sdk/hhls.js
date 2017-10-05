/// <reference path="jQuery-2.1.3.min.js" />

/// <reference path="json.js" />
/// <reference path="baiduTpls.js" />
/*
    本地服务
*/
var hhls = {
    callBack: function (aCallback, aPara) {
        try {
            if (aCallback) {
                if (aPara) {
                    aCallback(aPara);
                }
                else {
                    aCallback(aPara);
                }
            }
        }
        catch (cer) {
            var m = cer.Message;
        }
    },
    getLocalRes: function (aResPathList, aCallback) {
        try {
            var aIndex = -1;
            var aResults = [];
            var getRes = function () {
                try {
                    aIndex++;
                    if (aIndex < aResPathList.length) {
                        var aUrl = aResPathList[aIndex];
                        var aItem = { Path: aUrl, Content: "" };
                        $.ajax({
                            url: aUrl,
                            cache: false,
                            success: function (aRes) {
                                aItem.Content = aRes;
                                aResults.push(aItem);
                                getRes();
                            },
                            error: function (a, b, c) {
                                aResults.push(aItem);
                                getRes();
                            }
                        });
                    }
                    else {
                        hhls.callBack(aCallback, aResults);
                    }
                }
                catch (cer) {; }
            }
            getRes();
        }
        catch (cer) {; }
    },
    clearElement: function (aSelector) {
        try {
            var aSubs = $(aSelector + " *");
            $.each(aSubs, function (aIndex, aItem) {
                $(aItem).remove();
            });
            $(aSelector).empty();
        }
        catch (cer) {; }
    },
    removeElement: function (aSelector) {
        try {
            if ($(aSelector).length > 0) {
                hhls.clearElement(aSelector);
                $(aSelector).remove();
            }
        }
        catch (cer) {; }
    },
    fillElement: function (aSelector, aHtml) {
        try {
            hhls.clearElement(aSelector);
            $(aSelector).html(aHtml);
        }
        catch (cer) {; }
    },
    getObjJson: function (aObj) {
        var aJson = "";
        try {
            aJson = JSON.stringify(aObj);
        }
        catch (e) {
            alert(e);
        }
        return aJson;
    },
    getJsonObj: function (aJson) {
        var aObj = null;
        try {
            aObj = eval('(' + aJson + ')');
        }
        catch (cer) {; }
        return aObj;
    },
    getClone: function (aObj) {
        return hhls.getJsonObj(hhls.getObjJson(aObj));
    }
    //返回GUID    SplitString:分隔字符串(如-)
    , getGuid: function (SplitString) {
        var aId = "";
        var aSplitString = (SplitString != null) ? SplitString : "";
        try {
            var S4 = function () {
                return (((1 + Math.random()) * 0x10000) | 0).toString(16).substring(1);
            };
            aId = (S4() + S4() + aSplitString + S4() + aSplitString + S4() + aSplitString + S4() + aSplitString + S4() + S4() + S4());
        }
        catch (cer) {; }
        return aId;
    }
    //返回URL参数
    , getUrlHashVal: function (aUrlPara) {
        var reg = new RegExp("(^|&)" + aUrlPara + "=([^&]*)(&|$)", "i");
        var r = window.location.hash.substr(1).match(reg);
        var Res = (r != null) ? unescape(r[2]) : "";
        return Res;
    } //返回URL参数
    , getUrlParam: function (aUrlPara) {
        var reg = new RegExp("(^|&)" + aUrlPara + "=([^&]*)(&|$)"); //构造一个含有目标参数的正则表达式对象 
        var r = window.location.search.substr(1).match(reg);  //匹配目标参数 
        if (r != null) return unescape(r[2]); return ""; //返回参数值 
    }
    //返回URL参数
    , getUrlParamByDefault: function (aUrlPara, aDefault) {
        var reg = new RegExp("(^|&)" + aUrlPara + "=([^&]*)(&|$)"); //构造一个含有目标参数的正则表达式对象 
        var r = window.location.search.substr(1).match(reg);  //匹配目标参数 
        var aRes = aDefault;
        if (r != null) aRes = unescape(r[2]);
        return aRes; //返回参数值 
    }
    //复制属性
    , CopyPropertys: function (aSrc, aTarget) {
        try {
            for (var p in aSrc) {
                if (aTarget[p]) {
                    aTarget[p] = aSrc[p];
                }
            }
        }
        catch (cer) {; }
    }
    , GetTpls: function (aTpls, aCallback) {
        try {
            var aPs = [];
            var Index = 0;
            for (var p in aTpls) {
                aPs.push(aTpls[p]);
            }
            var aParaPaths = [];
            for (var i = 0; i < aPs.length; i++) {
                aParaPaths.push(aPs[i].P);
            }
            hhls.getLocalRes(aParaPaths, function (aRes) {
                for (var i = 0; i < aRes.length; i++) {
                    aPs[i].C = aRes[i].Content;
                }
                hhls.callBack(aCallback);
            });
        }
        catch (cer) {; }
    },
    //设置两个数组的主从关系
    setRelation: function (aMas, aDtl, aItemFieldName, aFun) {
        try {
            for (var i = 0; i < aMas.length; i++) {
                var aMaster = aMas[i];
                var aSubItems = $.grep(aDtl, function (aItem, aIndex) {
                    var aFlag = aFun(aMaster, aItem);
                    return aFlag;
                });
                aMaster[aItemFieldName] = aSubItems;
            }
        }
        catch (cer) {; }
    },
    //设置两个数组的主从关系(主外键相等)
    setRelationByField: function (aMas, aDtl, aItemFieldName, aMasField, aDtlField) {
        try {
            var aFun = function (aMaster, aItem) {
                var aFlag = aMaster[aMasField] == aItem[aDtlField];
                return aFlag;
            }
            hhls.setRelation(aMas, aDtl, aItemFieldName, aFun);
        }
        catch (cer) {; }
    },
    getIndex: function (aArray, aFun) {
        var aIndex = -1;
        try {
            for (var i = 0; i < aArray.length; i++) {
                var aFlag = aFun(aArray[i], i);
                if (aFlag) {
                    aIndex = i;
                    break;
                }
            }
        }
        catch (cer) {; }
        return aIndex;
    },
    getIndexByFieldValue: function (aArray, aField, aValue) {
        var aIndex = -1;
        try {
            var aFun = function (aItem, aItemIndex) {
                var aFlag = false;
                try {
                    aFlag = aItem[aField] == aValue;
                }
                catch (cee) {; }
                return aFlag;
            }
            return hhls.getIndex(aArray, aFun);
        }
        catch (cer) {; }
        return aIndex;
    },
    getSubItems: function (aArray, aFun) {
        var aRes = [];
        try {
            aRes = $.grep(aArray, aFun);
        }
        catch (cer) {; }
        return aRes;
    },
    setActive: function (aElSelector, aField, aKeyVal) {
        try {
            var aItems = $(aElSelector);
            aItems.removeClass("active");
            $.each(aItems, function (aInd, aItem) {
                try {
                    var aFlag = $(aItem).attr(aField) == aKeyVal;
                    if (aFlag) {
                        $(aItem).addClass("active");
                    }
                }
                catch (ee) {; }
            });
        }
        catch (cer) {; }
    },
    setActiveIndex: function (aElSelector, aIndex) {
        try {
            var aItems = $(aElSelector);
            aItems.removeClass("active");
            $(aItems[aIndex]).addClass("active");
        }
        catch (cer) {; }
    },
    getActiveVal: function (aElSelector, aField) {
        var aRes = { Ind: -1, Val: "" };
        try {
            var aItems = $(aElSelector);
            $.each(aItems, function (aInd, aItem) {
                try {
                    var aFlag = $(aItem).hasClass("active");
                    if (aFlag) {
                        aRes.Ind = aInd;
                        aRes.Val = $(aItem).attr(aField);
                    }
                }
                catch (ee) {; }
            });
        }
        catch (cer) {; }
        return aRes;
    },
    getSubItemsByFieldValue: function (aArray, aField, aValue) {
        var aRes = [];
        try {
            var aFun = function (aItem, aIndex) {
                var aFlag = false;
                try {
                    aFlag = aItem[aField] == aValue;
                }
                catch (cer) {; }
                return aFlag;
            }
            aRes = $.grep(aArray, aFun);
        }
        catch (cer) {; }
        return aRes;
    },
    getFirstSubItem: function (aArray, aFun) {
        var aRes = null;
        try {
            var aArrs = hhls.getSubItems(aArray, aFun);
            if (aArrs.length > 0) {
                aRes = aArrs[0];
            }
        }
        catch (cer) {; }
        return aRes;
    },
    getFirstSubItemByFieldValue: function (aArray, aField, aValue) {
        var aRes = null;
        try {
            var aArrs = hhls.getSubItemsByFieldValue(aArray, aField, aValue);
            if (aArrs.length > 0) {
                aRes = aArrs[0];
            }
        }
        catch (cer) {; }
        return aRes;
    },
    GetModalDlg: function (aDlgSelector, aHtml, aOnShow) {
        var aDlg = null;
        try {
            hhls.removeElement(aDlgSelector);
            var aDlg = $(aHtml).appendTo($("body"));;
            aDlg.unbind("shown.bs.modal").bind("shown.bs.modal", function () {
                try {
                    if (aOnShow) {
                        aOnShow(aDlg);
                    }
                }
                catch (cer) {; }
            });
        }
        catch (cer) {; }
        return aDlg;
    },
    isWeixBrowser: function () {
        var ua = navigator.userAgent.toLowerCase();
        if (ua.match(/MicroMessenger/i) == 'micromessenger') {
            return true;
        } else {
            return false;
        }
    },
    saveLocalObj: function (aKey, aObj) {
        try {
            window.localStorage.setItem(aKey, hhls.getObjJson(aObj));
        }
        catch (cer) {; }
    },
    loadLocalObj: function (aKey) {
        var aObj = null;
        try {
            var ajson = window.localStorage.getItem(aKey);
            aObj = hhls.getJsonObj(ajson);
        }
        catch (cer) { aObj = null; }
        return aObj;
    },
    assignSelect: function (aSelector, aDataSource, aKeyField, aCaptionField) {
        var aObj = $(aSelector);
        try {
            if (aObj.length > 0) {
                var aHtml = "";
                $.each(aDataSource, function (aInd, aItem) {
                    aHtml += "<option value='" + aItem[aKeyField] + "'>" + aItem[aCaptionField] + "</option>";
                });
                aObj.html(aHtml);
            }
        }
        catch (cer) { }
        return aObj;
    },
    goUrl: function (aUrl) {
        try {
            var aNewUrl = aUrl;
            aNewUrl += aUrl.indexOf("?") >= 0 ? "&" : "?";
            aNewUrl += "rndurl=" + Math.random();
            window.location.href = aNewUrl;
        }
        catch (cer) { }
    }
};

String.prototype.replaceAll = function (reallyDo, replaceWith, ignoreCase) {
    if (!RegExp.prototype.isPrototypeOf(reallyDo)) {
        return this.replace(new RegExp(reallyDo, (ignoreCase ? "gi" : "g")), replaceWith);
    } else {
        return this.replace(reallyDo, replaceWith);
    }
}

/*
private const double x_pi = 3.14159265358979324 * 3000.0 / 180.0;

/// <summary>

/// 中国正常坐标系GCJ02协议的坐标，转到 百度地图对应的 BD09 协议坐标

/// </summary>

/// <param name="lat">维度</param>

/// <param name="lng">经度</param>

public static void Convert_GCJ02_To_BD09(ref double lat,ref double lng)

{

double x = lng, y = lat;

double z =Math.Sqrt(x * x + y * y) + 0.00002 * Math.Sin(y * x_pi);

double theta = Math.Atan2(y, x) + 0.000003 * Math.Cos(x * x_pi);

lng = z * Math.Cos(theta) + 0.0065;

lat = z * Math.Sin(theta) + 0.006;

}

/// <summary>

/// 百度地图对应的 BD09 协议坐标，转到 中国正常坐标系GCJ02协议的坐标

/// </summary>

/// <param name="lat">维度</param>

/// <param name="lng">经度</param>

public static void Convert_BD09_To_GCJ02(ref double lat, ref double lng)

{

double x = lng - 0.0065, y = lat - 0.006;

double z = Math.Sqrt(x * x + y * y) - 0.00002 * Math.Sin(y * x_pi);

double theta = Math.Atan2(y, x) - 0.000003 * Math.Cos(x * x_pi);

lng = z * Math.Cos(theta);

lat = z * Math.Sin(theta);

} 
*/