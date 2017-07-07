<#ftl encoding='UTF-8'/>
<#assign tiles=JspTaglibs["http://tiles.apache.org/tags-tiles"]>
<@tiles.insertAttribute name="head"/>
<@tiles.insertAttribute name="localhead"/>
</head>
<body>
<@tiles.insertAttribute name="title" />
<@tiles.insertAttribute name="header"/>
<div class="main">
<#if login?exists>
  <#assign login = "${login!}">
  <#if login == "true">
  	<@tiles.insertAttribute name="menu" />
  </#if>
</#if>
<table width="99%" border="0" cellpadding="0" cellspacing="0">
   <tbody>
   		<tr>
           <td width="500px">
  				<div class="rightPane">
   					<@tiles.insertAttribute name="body" />
  				</div>
  		   </td>
  		</tr>
</table>
 </div>
<@tiles.insertAttribute name="footer" />
</body>
</html>
