//一開始就執行
$(document).ready(function () {
//	$("input").keydown(function (event) {
//	    if (event.which == 13) {
//	    	loginSubmit();
//	    }
//	});
//	
//	
//	$("#tt span").on("click", function(){
//		var treeUl = $(this).parent().parent().parent();
//		if($(treeUl).attr('style') == 'display:block'){
////			console.log($(treeUl).attr('style'));
////			$(treeUl).attr('style','display:none');
////			console.log($(this).attr('class'));
////			console.log("SSS");
//		}else{
////			$(treeUl).attr('style','display:block');
//		}
////		
////		var treeDiv = $(this).parent().parent().parent();
////		
////		
////		if('display:block' == $(treeDiv).attr('style')){
////			
//////			$(treeDiv).attr('display','none');
////		}
//	});
//	
//	
//	
//	
//	
//	
	
	
});



function errMsg(){
	$("#errMsg").remove();
}

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
		},
		success : function(obj) {
			result=obj
		},
		error : function(e) {
			console.log("error");
		}
	}).done(
		function() {
			var data ="";
			$.each(result.admCategoryAudienceAnalyzeList, function(index,obj) {
				console.log(obj);
				
				var id = obj.id;
				var recordDate = '2017-07-07';//obj.recordDate;
				var keyId = obj.keyId;
				var keyName =  obj.keyName;
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
		}
	);
	return false;
}

//分頁計算
function recordCount(){
//	$("#audiencet-tbody").empty();
	
	var result = null;
	$.ajax({
		url : "/AkbDmp/adm/queryrecordcount",
		type : 'POST',
		data : {
			"keyType" : $("#key-type-combobox").val(),
			"userType": $("#user-type-combobox").val(),
			"source": $("#source-combobox").val(),
			"recordDate": $("#record-date-textbox").val(),
			"keyId": $("#key-id-textbox").val(),
			
		},
		success : function(obj) {
			result=obj
		},
		error : function(e) {
			console.log("error");
		}
	}).done(
		function() {
			var data ="";
			$.each(result.admCategoryAudienceAnalyzeList, function(index,obj) {
				console.log(obj);
				
				var id = obj.id;
				var recordDate = '0707';//obj.recordDate;
				var keyId = obj.keyId;
				var keyName =  obj.keyName;
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
			var value = $( this ).val();
			$( "#page-total-textbox" ).text(result.pageSize);
		}
	);
	return false;
	
	
	
}

//function queryAudienceanAlyze(){
//	$("#audiencet-tbody").empty();
//	
//	var result = null;
//	$.ajax({
//		url : "/AkbDmp/adm/queryaudienceanalyze",
//		type : 'POST',
//		data : {
//			"keyType" : "2"
//		},
//		success : function(obj) {
//			console.log(obj);
//			result = $.parseJSON(obj)
//		},
//		error : function(e) {
//			console.log("error");
//		}
//	}).done(
//		function() {
//			var array = result.admCategoryAudienceAnalyzeList;
//			var data ="";
//			$.each(array, function(index,obj) {
//				console.log( obj.id);
//				
//				var id = obj.id;
////				var recordDate = obj.recordDate;
//				var recordDate = '0707';
//				var keyId = obj.keyId;
//				var keyName =  obj.keyName;
//				var keyType =  obj.keyType;
//				var userType =  obj.userType;
//				var source =  obj.source;
//				var keyCount =  obj.keyCount;
//				
//				data = data + '<tr id="datagrid-row-r1-2-0" datagrid-row-index="0" class="datagrid-row">'+
//					   '<td field="序號"><div style="height:auto;" class="datagrid-cell datagrid-cell-c1-itemid">'+id+'</div></td>'+
//					   '<td field="紀錄日期"><div style="height:auto;" class="datagrid-cell datagrid-cell-c1-itemid">'+recordDate+'</div></td>'+
//					   '<td field="分類序號"><div style="height:auto;" class="datagrid-cell datagrid-cell-c1-itemid">'+keyId+'</div></td>'+
//					   '<td field="分類名稱"><div style="height:auto;" class="datagrid-cell datagrid-cell-c1-itemid">'+keyName+'</div></td>'+
//					   '<td field="分類型態"><div style="height:auto;" class="datagrid-cell datagrid-cell-c1-itemid">'+keyType+'</div></td>'+
//					   '<td field="受眾類型"><div style="height:auto;" class="datagrid-cell datagrid-cell-c1-itemid">'+userType+'</div></td>'+
//					   '<td field="來源"><div style="height:auto;" class="datagrid-cell datagrid-cell-c1-itemid">'+source+'</div></td>'+
//					   '<td field="受眾數"><div style="height:auto;" class="datagrid-cell datagrid-cell-c1-itemid">'+keyCount+'</div></td></tr>'
//			});
//			$('#audiencet-tbody').append(data);
//		}
//	);
//	return false;
//}


