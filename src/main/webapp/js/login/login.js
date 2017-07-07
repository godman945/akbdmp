//一開始就執行
$(document).ready(function () {

	if(localStorage.getItem("pchome_dmp_adm") != null){
		var result = null;
		var test = localStorage.getItem("pchome_dmp_adm");
		console.log(test);
		$.ajax({
			url : "http://localhost:8080/AkbDmp/index.html",
			type : 'GET',
			data : {
				'localStorage' : test
			},
			success : function(obj) {
				window.location.href='http://localhost:8080/AkbDmp/index.html';
			},
			error : function(e) {
				console.log("error");
			}
		}).done(

		);
	}
});


function loginSubmit(){
	if($("#account").val() == "" && $("#errMsg").text() == ""){
		$("#account").parent().append('<div id="errMsg" class="error-message">帳號不可為空</div>');
		setTimeout('errMsg()',3000);
		return false;
	}else if($("#password").val() == "" && $("#errMsg").text() == ""){
		$("#password").after('<div id="errMsg" class="error-message">密碼不可為空</div>');
		setTimeout('errMsg()',3000);
		return false;
	}else{
		var result = null;
		$.ajax({
			url : "http://localhost:8080/AkbDmp/checklogin",
			type : 'POST',
			data : {
				'account' : $("#account").val(),
				'password' : $("#password").val(),
			},
			success : function(obj) {
				console.log(obj);
				result = $.parseJSON(obj)
			},
			error : function(e) {
				console.log("error");
			}
		}).done(
				function() {
					if (result.result == "FAIL") {
						if(result.msg == "not found"){
							$("#password").after('<div id="errMsg" class="error-message">登入錯誤</div>');
							setTimeout('errMsg()',3000);
							return false;
						}
					}else{
						localStorage.setItem("pchome_dmp_adm", result.msg);
						$("#password").after('<div id="errMsg" class="error-message">登入OK</div>');
						setTimeout('errMsg()',3000);
						location.reload();
						return false;
					}
				}
		);
	}
}

function errMsg(){
	$("#errMsg").remove();
}

