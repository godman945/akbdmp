<#ftl encoding='UTF-8'/>
<div class="panel panel-htop" style="display: block;">
<div id="demo" data-options="href:'../../easyui/demo/panel/basic.html',border:false,onLoad:onLoad" style="padding: 20px; overflow: auto;  min-height: 350px;" title="" class="panel-body panel-body-noheader panel-body-noborder">
	<div class="panel panel-htop" style="display: block; width: 92%;"><div class="panel-header" ><div class="panel-title">樣版</div><div class="panel-tool"></div></div><div id="p" class="easyui-panel panel-body" title="" style="padding: 10px;  height: 100%;">
	
	
		<div>
			Date From: <input class="easyui-datebox datebox-f combo-f textbox-f" style="width: 80px; display: none;"><span class="textbox combo datebox" style="width: 78px;"><span class="textbox-addon textbox-addon-right" style="right: 0px; top: 0px;"><a href="javascript:;" class="textbox-icon combo-arrow" icon-index="0" tabindex="-1" style="width: 18px; height: 22px;"></a></span><input id="_easyui_textbox_input2" type="text" class="textbox-text validatebox-text textbox-prompt" autocomplete="off" tabindex="0" placeholder="" style="margin: 0px 18px 0px 0px; padding-top: 0px; padding-bottom: 0px; height: 22px; line-height: 22px; width: 52px;"><input type="hidden" class="textbox-value" name="" value=""></span>
			To: <input class="easyui-datebox datebox-f combo-f textbox-f" style="width: 80px; display: none;"><span class="textbox combo datebox" style="width: 78px;"><span class="textbox-addon textbox-addon-right" style="right: 0px; top: 0px;"><a href="javascript:;" class="textbox-icon combo-arrow" icon-index="0" tabindex="-1" style="width: 18px; height: 22px;"></a></span><input id="_easyui_textbox_input3" type="text" class="textbox-text validatebox-text textbox-prompt" autocomplete="off" tabindex="0" placeholder="" style="margin: 0px 18px 0px 0px; padding-top: 0px; padding-bottom: 0px; height: 22px; line-height: 22px; width: 52px;"><input type="hidden" class="textbox-value" name="" value=""></span>
			Language: 
			<input class="easyui-combobox combobox-f combo-f textbox-f" style="width: 100px; display: none;" url="data/combobox_data.json" valuefield="id" textfield="text"><span class="textbox combo" style="width: 98px;"><span class="textbox-addon textbox-addon-right" style="right: 0px; top: 0px;"><a href="javascript:;" class="textbox-icon combo-arrow" icon-index="0" tabindex="-1" style="width: 18px; height: 22px;"></a></span><input id="_easyui_textbox_input1" type="text" class="textbox-text validatebox-text" autocomplete="off" tabindex="0" placeholder="" style="margin: 0px 18px 0px 0px; padding-top: 0px; padding-bottom: 0px; height: 22px; line-height: 22px; width: 72px;"><input type="hidden" class="textbox-value" name="" value="3"></span>
			<a href="#" class="easyui-linkbutton l-btn l-btn-small" iconcls="icon-search" group="" id=""><span class="l-btn-left l-btn-icon-left"><span class="l-btn-text">Search</span><span class="l-btn-icon icon-search">&nbsp;</span></span></a>
		</div>
		
		<table style="border:3px #cccccc solid;" cellpadding="10" border='1'>
				<tbody>
				 <tr>
			      <th>序號</th>
			      <th>記錄時間</th>
			      <th>分類序號</th>
			      <th>分類名稱</th>
			      <th>分類型態[1:小分類,2:大分類,3:性別,4.年齡區間]</th>
			      <th>受眾類型</th>
			      <th>來源</th>
			      <th>受眾數</th>
    			</tr>
				<#if admCategoryAudienceAnalyzeList?exists>
				<#list admCategoryAudienceAnalyzeList as admCategoryAudienceAnalyze>
					<tr id="datagrid-row-r1-2-0" datagrid-row-index="0" class="datagrid-row">              		 
						<td field="序號">	
							<div style="height:auto;" class="datagrid-cell datagrid-cell-c1-itemid">
								${admCategoryAudienceAnalyze.id}
							</div>
						</td>
						
						<td field="紀錄日期">	
							<div style="height:auto;" class="datagrid-cell datagrid-cell-c1-itemid">
								${admCategoryAudienceAnalyze.recordDate}
							</div>
						</td>
						<td field="分類序號">	
							<div style="height:auto;" class="datagrid-cell datagrid-cell-c1-itemid">
								${admCategoryAudienceAnalyze.keyId}
							</div>
						</td>
						<td field="分類序號">	
							<div style="height:auto;" class="datagrid-cell datagrid-cell-c1-itemid">
								${admCategoryAudienceAnalyze.keyName}
							</div>
						</td>
						<td field="分類序號">	
							<div style="height:auto;" class="datagrid-cell datagrid-cell-c1-itemid">
								${admCategoryAudienceAnalyze.keyType}
							</div>
						</td>
						<td field="分類序號">	
							<div style="height:auto;" class="datagrid-cell datagrid-cell-c1-itemid">
								${admCategoryAudienceAnalyze.userType}
							</div>
						</td>
						<td field="分類序號">	
							<div style="height:auto;" class="datagrid-cell datagrid-cell-c1-itemid">
								${admCategoryAudienceAnalyze.source}
							</div>
						</td>
						<td field="分類序號">	
							<div style="height:auto;" class="datagrid-cell datagrid-cell-c1-itemid">
								${admCategoryAudienceAnalyze.keyCount}
							</div>
						</td>
					</tr>
               	</#list>
			</#if>
			</tbody>
		</table>

		
		
		
			
</div>
</div>
</div>