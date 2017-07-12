 <#ftl encoding='UTF-8'/>
    <div  title="" style="width:100%;max-width:400px;margin-left:30%;">
        <div style="margin-bottom:20px">
            <label for="account" class="label-top">帳號:</label>
            <input id="account" class="easyui-validatebox tb" data-options="required:true,validateOnCreate:false,validateOnBlur:true" style="width:500px;">
        </div>
        <div style="margin-bottom:20px">
            <label for="password" class="label-top">密碼:</label>
            <input id="password" class="easyui-validatebox tb" data-options="required:true,validateOnCreate:false,validateOnBlur:true" style="width:500px;>
        </div>
    </div>

	<div style="margin:20px 0" id="submitDiv">
		<a href="#" onclick="loginSubmit()"; class="easyui-linkbutton c8 l-btn l-btn-small" style="width:500px;margin-top:30px;" group="" id=""><span class="l-btn-left" style="margin-top: 0px;"><span class="l-btn-text">登入</span></span></a>
	</div>
		
	<div style="color:red;">${ERR!}<div>