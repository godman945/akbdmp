

var res=document.URL;
var docurl = encodeURIComponent(res);
var keywordValue="";
var pageValue="";
var pid="";
var ptype="";
var seway="";

document.write('<div id="veo" width="300" height="250"><iframe id="f1" src="videoTest10.html" width="300" height="250" allowtransparency="true" frameborder="0" scrolling="no"></iframe></div>');

//var document = document;


//var imported = document.createElement('script');
//imported.src = 'http://localhost:8080/AkbDmp/js/alex.js';
//document.head.appendChild(imported);



/*
var res=document.URL;
var docurl = encodeURIComponent(res);
var keywordValue="";
var pageValue="";
var pid="";
var ptype="";
var seway="";


if (typeof pad_pchad != 'object') {
	pad_pchad=[];
}

if(typeof pad_precise != 'undefined' ){
	seway=pad_precise;
}else{
	
	seway=false;
}

if(typeof pad_positionId != 'undefined' ){
	pid=pad_positionId.substring(0,16);
	ptype=pad_positionId.substring(16,17);
	
	pad_pchad.push(pid);

	if(ptype==""){
		ptype="C";
	}

}

if(ptype=="S"){

	if(typeof pad_keyword != 'undefined' ){
		keywordValue=pad_keyword;
	}

	if(typeof pad_page != 'undefined' ){
		pageValue=pad_page;
	}
	
	if(keywordValue.length==0){
		
		//test
		//res="http://search.pchome.com.tw/search/?q=%E6%89%8B%E6%A9%9F%E6%AE%BC&ch=&ac="
			
	
		if(res.indexOf("nicolee.pchome.com.tw") > 1){
        
			//http://nicolee.pchome.com.tw:8080/akbadm_git/adteststg.jsp?q=usb&page=1&precise=false;
		
			var testurl=res;	
			
				
			var kis=testurl.indexOf("q=");
			
					
			if(kis>1){
				
				var pis=testurl.indexOf("page=");
			    var tis=testurl.indexOf("precise=");
				keywordValue=testurl.substring(kis+2,pis-1);
			
				if(pis < 1){
					pageValue=1;
				}else{
					pageValue=testurl.substring(pis+5,tis-1);	
				}
			
				//alert(kis+","+tis+","+pis+","+keywordValue+","+pageValue);
			}
		}
	
		if(res.indexOf("search.pchome.com.tw") > 1){
			
			//alert("search");
	        
			//http://search.pchome.com.tw/search/?q=%E4%B8%AD%E6%96%87&ch=&ac=
		
			var testurl=res;	
			
				
			var kis=testurl.indexOf("q=");
			
					
			if(kis>1){
				
				var pis=testurl.indexOf("ch=");
			    //var tis=testurl.indexOf("precise=");
				keywordValue=testurl.substring(kis+2,pis-1);
				
			
				
			
				//alert(kis+","+pis+","+keywordValue);
			}
		}
		
	    if(res.indexOf("search.ruten.com.tw") > 1){
	        
	    	  // http://search.ruten.com.tw/search/s000.php?searchfrom=indexbar&k=ipad&t=0&p=4
			
			var testurl=res;	
			
			var kis=testurl.indexOf("k=");
			
			if(kis>1){
				
				var tis=testurl.indexOf("t=");
				var pis=testurl.indexOf("p=");
			
				keywordValue=testurl.substring(kis+2,tis-1);
				//keywordValue=document.getElementById("kwd").value;
				if(pis < 1){
					pageValue=1;
				}else{
					pageValue=testurl.substring(pis+2,testurl.length);	
				}
			
				//alert(kis+","+tis+","+pis+","+keywordValue+","+pageValue);
			}
		}
       
	}
	
	
	

}else{
	
	keywordValue="";
	pageValue="";
	seway="";
	
	
}



var adurl="https://kdcl.pchome.com.tw/adshow2.html?pfbxCustomerInfoId="+pad_customerId;
	adurl+="&positionId="+pid;
	adurl+="&padWidth="+pad_width;
	adurl+="&padHeight="+pad_height;
	adurl+="&keyword="+keywordValue;
	adurl+="&page="+pageValue;
	adurl+="&precise="+seway;
	adurl+="&t="+Math.floor(Math.random() * 1000 + 1);
	
	//補版第2次呼叫不傳 docurl encoder 有問題
	if(docurl.indexOf("kdcl") > 1 || docurl.indexOf("kwstg") > 1){
		adurl+="&docurl=";
	}else{
		adurl+="&docurl="+docurl;
	}



var showadscript = "<scr" + "ipt type=text/javascript src="+adurl+"></scr" + "ipt>";

if(pad_pchad.length <= 10){

	if(ptype=="S"){
		//search no iframe
		document.write(showadscript);	
	}else{
    	document.write('<iframe class="akb_iframe" scrolling="no" frameborder="0" marginwidth="0" marginheight="0" vspace="0" hspace="0" id="pchome8044_ad_frame1" width="'+pad_width+'" height="'+pad_height+'" allowtransparency="true" allowfullscreen="true" src="javascript:\''+showadscript+'\'"></iframe>');
	
	}
	
}else{
	
	alert("超過廣告上限，最多只能貼10則廣告!");
}

*/

