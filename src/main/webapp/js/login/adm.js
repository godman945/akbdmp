//一開始就執行
$(document).ready(function () {

});


function loginOut(){
	localStorage.removeItem("pchome_dmp_adm");
	window.location.href='http://localhost:8080/AkbDmp/login.html';
}

function errMsg(){
	$("#errMsg").remove();
}

