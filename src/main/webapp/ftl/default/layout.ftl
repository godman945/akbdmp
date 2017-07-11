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
	<div class="rightPane">
  		<div style="padding-top:15px;">
    		<div id="p" class="easyui-panel" title="<#if title?exists><#assign title = "${title!}">${title!}<#else>你忘記傳title了!</#if>" style="height:200px;padding:10px;">
				<div>   					
					<@tiles.insertAttribute name="body" />
				</div>
  			</div>
  		</div>
	</div>
</div>
<@tiles.insertAttribute name="footer" />
</body>
</html>
