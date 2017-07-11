<#ftl encoding='UTF-8'/>
<div class="header">
 <table width="100%" border="0" cellpadding="0" cellspacing="0">
                <tbody>
                	<tr>
	                    <td>
	                        <img src="../imgs/logo_pchome.gif" border="0"></a>
	                        <img src="../imgs/logo.gif" border="0"></a>
	                    </td>
	                    <#if login?exists>
						  <#assign login = "${login!}">
						  <#if login == "true">
	                    <td align="right">
	                        <div class="t13"> 使用者：<br>
	                            <strong><img src="../imgs/webicon15.gif" width="16" height="15" align="absmiddle"> admin@staff.pchome.com.tw </strong>(<a href="#" onclick="loginOut()">登出</a>)
	                        </div>
	                    </td>
	                     </#if>
						</#if>
                	</tr>
                	<tr>
                    	<td height="3" colspan="2" bgcolor="#3D7CD3"></td>
                	</tr>
            </tbody></table>
 </div>

 