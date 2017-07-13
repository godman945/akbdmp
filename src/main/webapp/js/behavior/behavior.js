//一開始就執行
$(document).ready(function () {
	
});

function search(){
	$("#audiencet-tbody").empty();
	var result = null;
	
	$.ajax({
		url : "/AkbDmp/adm/queryaudienceanalyze",
		type : 'POST',
		data : {
			"keyType" : $("#key-type-combobox").val(),
			"userType": $("#user-type-combobox").val(),
			"source": $("#source-combobox").val(),
			"recordDate": $("#record-date-textbox").val(),
			"keyId": $("#key-id-textbox").val(),
			"page": $('#total-pages-select').find(":selected").text(),
			"pageSize": $('#record-unit-select').find(":selected").text(),
		},
		success : function(obj) {
			result=obj
			console.log(result);
			
		},
		error : function(e) {
			console.log("error");
		}
	}).done(
		function() {
			if(result.admCategoryAudienceAnalyzeList.length > 0){
				var data ="";
				$.each(result.admCategoryAudienceAnalyzeList, function(index,obj) {
					console.log(obj);
					
					var id = obj.id;
					var recordDate = '2017-07-07';// obj.recordDate;
					var keyId = obj.keyId;
					var keyName =  obj.keyName == "null" ? "" : obj.keyName;
					var keyType =  obj.keyType;
					var userType =  obj.userType;
					var source =  obj.source;
					var keyCount =  obj.keyCount;
					
					data = data + '<tr id="datagrid-row-r1-2-0" datagrid-row-index="0" class="datagrid-row">'+
						   '<td field="序號"><div style="height:auto;" class="datagrid-cell datagrid-cell-c1-itemid">'+id+'</div></td>'+
						   '<td field="紀錄日期"><div style="height:auto;" class="datagrid-cell datagrid-cell-c1-itemid">'+recordDate+'</div></td>'+
						   '<td field="分類序號"><div style="height:auto;" class="datagrid-cell datagrid-cell-c1-itemid">'+keyId+'</div></td>'+
						   '<td field="分類名稱"><div style="height:auto;" class="datagrid-cell datagrid-cell-c1-itemid">'+keyName+'</div></td>'+
						   '<td field="分類型態"><div style="height:auto;" class="datagrid-cell datagrid-cell-c1-itemid">'+keyType+'</div></td>'+
						   '<td field="受眾類型"><div style="height:auto;" class="datagrid-cell datagrid-cell-c1-itemid">'+userType+'</div></td>'+
						   '<td field="來源"><div style="height:auto;" class="datagrid-cell datagrid-cell-c1-itemid">'+source+'</div></td>'+
						   '<td field="受眾數"><div style="height:auto;" class="datagrid-cell datagrid-cell-c1-itemid">'+keyCount+'</div></td></tr>'
				});
				$('#audiencet-tbody').append(data);
				console.log(result.pageSize)
				$('#total-count').text(result.pageSize);
				// 共幾頁
				totalPages();
			}else{
				$("#total-pages-select").empty();
				$("#total-pages-select").append("<option>1</option>");
				alert("查無資料")
			}
		}
	);
	return false;
}

function totalPages(){
	var totalPages= Math.ceil(Number($('#total-count').text())/Number($('#record-unit-select').find(":selected").text()));
	
	var page =0;
	var optionsAsString = "";
	for(var i = 1; i <= totalPages; i++) {
		page=page+1;
	    optionsAsString += "<option value='" + page.toString() + "'>" + page.toString() + "</option>";
	}
	$("#total-pages-select").empty();
	$("#total-pages-select").append( optionsAsString );
	$('#total-pages-text').text(page);
	
	
	return false;
}

function errMsg(){
	$("#errMsg").remove();
}

//// 分頁計算
//function recordCount(){
//	
//	var result = null;
//	$.ajax({
//		url : "/AkbDmp/adm/queryrecordcount",
//		type : 'POST',
//		data : {
//			"keyType" : $("#key-type-combobox").val(),
//			"userType": $("#user-type-combobox").val(),
//			"source": $("#source-combobox").val(),
//			"recordDate": $("#record-date-textbox").val(),
//			"keyId": $("#key-id-textbox").val(),
//			
//		},
//		success : function(obj) {
//			result=obj
//			console.log(result)
//		},
//		error : function(e) {
//			console.log("error");
//		}
//	}).done(
//		function() {
//			
//				var data ="";
//				$.each(result.admCategoryAudienceAnalyzeList, function(index,obj) {
//					console.log(obj);
//					
//					var id = obj.id;
//					var recordDate = '0707';// obj.recordDate;
//					var keyId = obj.keyId;
//					var keyName =  obj.keyName;
//					var keyType =  obj.keyType;
//					var userType =  obj.userType;
//					var source =  obj.source;
//					var keyCount =  obj.keyCount;
//					
//					data = data + '<tr id="datagrid-row-r1-2-0" datagrid-row-index="0" class="datagrid-row">'+
//						   '<td field="序號"><div style="height:auto;" class="datagrid-cell datagrid-cell-c1-itemid">'+id+'</div></td>'+
//						   '<td field="紀錄日期"><div style="height:auto;" class="datagrid-cell datagrid-cell-c1-itemid">'+recordDate+'</div></td>'+
//						   '<td field="分類序號"><div style="height:auto;" class="datagrid-cell datagrid-cell-c1-itemid">'+keyId+'</div></td>'+
//						   '<td field="分類名稱"><div style="height:auto;" class="datagrid-cell datagrid-cell-c1-itemid">'+keyName+'</div></td>'+
//						   '<td field="分類型態"><div style="height:auto;" class="datagrid-cell datagrid-cell-c1-itemid">'+keyType+'</div></td>'+
//						   '<td field="受眾類型"><div style="height:auto;" class="datagrid-cell datagrid-cell-c1-itemid">'+userType+'</div></td>'+
//						   '<td field="來源"><div style="height:auto;" class="datagrid-cell datagrid-cell-c1-itemid">'+source+'</div></td>'+
//						   '<td field="受眾數"><div style="height:auto;" class="datagrid-cell datagrid-cell-c1-itemid">'+keyCount+'</div></td></tr>'
//				});
//				$('#audiencet-tbody').append(data);
//				var value = $( this ).val();
//				$( "#page-total-textbox" ).text(result.pageSize);
//		}
//	);
//	return false;
//}





