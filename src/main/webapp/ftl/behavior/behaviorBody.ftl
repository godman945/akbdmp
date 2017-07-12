<#ftl encoding='UTF-8'/>
			<!--Date From: <input class="easyui-datebox datebox-f combo-f textbox-f" style="width: 80px; display: none;"><span class="textbox combo datebox" style="width: 78px;"><span class="textbox-addon textbox-addon-right" style="right: 0px; top: 0px;"><a href="javascript:;" class="textbox-icon combo-arrow" icon-index="0" tabindex="-1" style="width: 18px; height: 22px;"></a></span><input id="_easyui_textbox_input2" type="text" class="textbox-text validatebox-text textbox-prompt" autocomplete="off" tabindex="0" placeholder="" style="margin: 0px 18px 0px 0px; padding-top: 0px; padding-bottom: 0px; height: 22px; line-height: 22px; width: 52px;"><input type="hidden" class="textbox-value" name="" value=""></span>-->
			紀錄日期:<input id="record-date-textbox" class="easyui-datebox" style="width:110px">
			受眾編號:<input id="key-id-textbox" type="text" style="width:100px">
			受眾型態:
			<select id="key-type-combobox" class="easyui-combobox" name="dept" style="width:100px;">
				<option value="">none</option>
			    <option value="1">1:小分類</option>
			    <option value="2">2:大分類</option>
			    <option value="3">3:性別</option>
			    <option value="4">4.年齡區間</option>
			</select> 
			
			受眾類型: 
			<select id="user-type-combobox" class="easyui-combobox" name="dept" style="width:100px;">
			    <option value="">none</option>
			    <option value="memid">memid</option>
			    <option value="uuid">uuid</option>
			</select>
			
			來源: 
			<select id="source-combobox" class="easyui-combobox" name="dept" style="width:100px;">
			    <option value="">none</option>
			    <option value="24h">24h</option>
			    <option value="ruten">ruten</option>
			    <option value="adclick">adclick</option>
			</select>

			<a href="#" class="easyui-linkbutton l-btn l-btn-small" iconcls="icon-search" group="" id="" ><span class="l-btn-text" onclick="search()">Search</span></a>
		</div>
		<h1>
		<table style="border:3px #cccccc solid;width:100%;" cellpadding="10" border='1' >
			<thead>
				 <tr>
			      <th>序號</th>
			      <th>紀錄日期</th>
			      <th>受眾編號</th>
			      <th>受眾名稱</th>
			      <th>受眾型態<br>[1:小分類,2:大分類,3:性別,4.年齡區間]</th>
			      <th>受眾類型</th>
			      <th>來源</th>
			      <th>受眾數</th>
    			</tr>
    		</thead>	
    		<tbody id="audiencet-tbody">
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
<div class="datagrid-pager pagination" style="background-color:lightsteelblue">
	<table cellspacing="0" cellpadding="0" border="0">
		<tbody>
			<tr>
				<td>
					<select class="pagination-page-list">
						<option>10</option>
						<option>20</option>
						<option>30</option>
						<option>40</option><option>50</option>
						</select>
				</td>
				<td>
					<div class="pagination-btn-separator">
					
					</div>
				</td>
				<td>
					<a href="javascript:;" class="l-btn l-btn-small l-btn-plain l-btn-disabled l-btn-plain-disabled" group="" id="">
						<span class="l-btn-left l-btn-icon-left">
						<span class="l-btn-text l-btn-empty">&nbsp;</span>
						<span class="l-btn-icon pagination-first">&nbsp;</span>
						</span>
					</a>
				</td>
				<td>
					<a href="javascript:;" class="l-btn l-btn-small l-btn-plain l-btn-disabled l-btn-plain-disabled" group="" id="">
						<span class="l-btn-left l-btn-icon-left"><span class="l-btn-text l-btn-empty">&nbsp;</span>
						<span class="l-btn-icon pagination-prev">&nbsp;</span></span>
					</a>
				</td>
				<td>
					<div class="pagination-btn-separator"></div>
				</td>
				<td><span style="padding-left:6px;">Page</span>
				</td>
				<td><input class="pagination-num" type="text" value="1" size="2">
				</td>
				<td>
					<span style="padding-right:6px;">of 1</span>
				</td>
				<td>
					<div class="pagination-btn-separator"></div>
				</td>
				<td>
					<a onclick="adb();" class="l-btn l-btn-small l-btn-plain l-btn-disabled l-btn-plain-disabled" group="" id="">
						<span class="l-btn-left l-btn-icon-left"><span class="l-btn-text l-btn-empty">&nbsp;</span>
						<span class="l-btn-icon pagination-next">&nbsp;</span></span>
					</a></td><td><a href="javascript:;" class="l-btn l-btn-small l-btn-plain l-btn-disabled l-btn-plain-disabled" group="" id=""><span class="l-btn-left l-btn-icon-left"><span class="l-btn-text l-btn-empty">&nbsp;</span><span class="l-btn-icon pagination-last">&nbsp;</span></span>
					</a></td><td><div class="pagination-btn-separator"></div></td><td><a href="javascript:;" class="l-btn l-btn-small l-btn-plain" group="" id=""><span class="l-btn-left l-btn-icon-left"><span class="l-btn-text l-btn-empty">&nbsp;</span><span class="l-btn-icon pagination-load">&nbsp;</span></span></a></td></tr></tbody></table><div class="pagination-info">總共6頁</div><div style="clear:both;">
		</div>
