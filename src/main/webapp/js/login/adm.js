//一開始就執行
$(document).ready(function () {
	$("input").keydown(function (event) {
	    if (event.which == 13) {
	    	loginSubmit();
	    }
	});

});



function loginOut(){
	$.ajax({
		url : "/AkbDmp/adm/userlogout",
		type : 'POST',
		data : {
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
			location.reload();
		}
	);
}

function errMsg(){
	$("#errMsg").remove();
}

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
			url : "/AkbDmp/adm/userlogin",
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
					location.reload();
				}
			}
		);
	}
}

function errMsg(){
	$("#errMsg").remove();
}