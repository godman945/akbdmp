var currentPage;		//全域的目前第幾頁
var currentPageSize; 	//全域的一頁幾筆
var currentPageText 	//全域的一頁幾筆
var totalCount;			//總筆數
var lastPage;			//最後一頁


//一開始就執行
$(document).ready(function () {
	
    $("#record-date-textbox").datebox({
		  formatter:function(date){
			var y=date.getFullYear();
			var m=date.getMonth()+1;
			var d=date.getDate();
			return y+'/'+(m<10?('0'+m):m)+'/'+(d<10?('0'+d):d);  
		  },
		  parser:function(s){ 	
			var t=Date.parse(s);
			if (!isNaN(t)) {
				return new Date(t);
			}else {
				return new Date();
			}
		  } 
	  });
	
	currentPage=Number($('#total-pages-select').find(":selected").text());
	currentPageSize=Number($('#record-unit-select').find(":selected").text());
	
	//一頁幾筆select
	$('#record-unit-select').change(function(){  
		currentPageSize=Number($('#record-unit-select').find(":selected").text());
		search();
	});
	
	
	//共幾頁select
	$('#total-pages-select').change(function(){  
		currentPage=Number($('#total-pages-select').find(":selected").text());
		search();
	});
	
	//上一頁btn
	$('#pre-page-btn').on('click', function(){
		if(currentPage <=1){
			return false;
		}
		currentPage=Number(currentPage)-1;
		removeSelected();
		$("#total-pages-select option[value='"+currentPage.toString()+"']").attr("selected",true);
//		alert(currentPage);
		search();
	});
	
	//下一頁btn
	$('#next-page-btn').on('click', function(){
//		alert("lastPage");
//		alert(lastPage);
		if(currentPage >= lastPage){
			return false;
		}	
		currentPage=Number(currentPage)+1;	
		removeSelected();
		$("#total-pages-select option[value='"+currentPage.toString()+"']").attr("selected",true);
//		alert(currentPage);
		search();
	});
	
	//第一頁btn
	$('#first-page-btn').on('click', function(){
		currentPage=Number(1);
		removeSelected();
		$("#total-pages-select option[value='"+currentPage.toString()+"']").attr("selected",true);
		search();
	});
	
	//最後一頁btn
	$('#last-page-btn').on('click', function(){
		currentPage=Number(lastPage);
		removeSelected();
		$("#total-pages-select option[value='"+currentPage.toString()+"']").attr("selected",true);
		search();
	});
	
	//重新查詢
	$('#reload-btn').on('click', function(){
		search()
	});
	
});

function removeSelected(){
	$('#total-pages-select').children().each(function(){
		$(this).attr("selected",false); //或是給selected也可
	});
};

function search(){
//	alert("currentPage")
//	alert(currentPage)
//	alert("format: "+$("#record-date-textbox").val())
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
				var i=0;
				$.each(result.admCategoryAudienceAnalyzeList, function(index,obj) {
					console.log(obj);
					
					var id = obj.id;
					var recordDate = obj.recordDate;
					var keyId = obj.keyId;
					var keyName =  obj.keyName == "null" ? "" : obj.keyName;
					var keyType =  obj.keyType;
					var userType =  obj.userType;
					var source =  obj.source;
					var keyCount =  obj.keyCount;
					i=i+1;
					
					data = data + '<tr id="datagrid-row-r1-2-0" datagrid-row-index="0" class="datagrid-row">'+
					'<td field="流水號"><div style="height:auto;" class="datagrid-cell datagrid-cell-c1-itemid">'+i+'</div></td>'+
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
				totalCount=Number(result.pageSize);
				// 共幾頁
				totalPages();
			}else{
				currentPage=Number(1);
				$("#total-pages-select").empty();
				$("#total-pages-select").append("<option>1</option>");
				$("#total-pages-select option[value='"+currentPage+"']").attr("selected","selected");
//				alert("查無資料");
				
				var data = '<tr id="datagrid-row-r1-2-0" datagrid-row-index="0" class="datagrid-row">'+
				'<td  colspan="9" field="查無資料"><div style="height:auto;" class="datagrid-cell datagrid-cell-c1-itemid">查無資料</div></td></tr>'
				$('#audiencet-tbody').append(data);
				$('#total-pages-text').text("0");
//				alert("currentPage");
//				alert(currentPage);
			}
		}
	);
	return false;
}

function totalPages(){
	var totalPages=Number(Math.ceil(totalCount/currentPageSize));
	lastPage=Number(totalPages);
	var page =0;
	var optionsAsString = "";
	for(var i = 1; i <= totalPages; i++) {
		page=page+1;
	    optionsAsString += "<option value=" + page + ">" + page.toString() + "</option>";
	}
	
	$("#total-pages-select").empty();
	$("#total-pages-select").append( optionsAsString );
	$('#total-pages-text').text(page);
	removeSelected()
	$("#total-pages-select option[value='"+currentPage+"']").attr("selected","selected");
	return false;
}


function errMsg(){
	$("#errMsg").remove();
}

