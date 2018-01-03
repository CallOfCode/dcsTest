layui.config({
	base : "js/"
}).use(['form','element','layer','jquery'],function(){
	var form = layui.form(),
		layer = parent.layer === undefined ? layui.layer : parent.layer,
		element = layui.element(),
		$ = layui.jquery;

	$(".panel a").on("click",function(){
		window.parent.addTab($(this));
	})

	//数字格式化
	$(".panel span").each(function(){
		$(this).html($(this).text()>9999 ? ($(this).text()/10000).toFixed(2) + "<em>万</em>" : $(this).text());	
	})

	//系统基本参数
	if(window.sessionStorage.getItem("systemParameter")){
		var systemParameter = JSON.parse(window.sessionStorage.getItem("systemParameter"));
		fillParameter(systemParameter);
	}else{
		systemParameter = {};
		systemParameter.version='Dalston.SR4';
        systemParameter.eureka='http://localhost:8761/eureka/';
        systemParameter.zuul='http://localhost:8762';
        systemParameter.zookeeper='172.17.24.99:2181';
        systemParameter.elasticsearch='172.17.24.99:9200';
        systemParameter.services='clean-service';
        fillParameter(systemParameter);
	}

	//填充数据方法
 	function fillParameter(data){
 		//判断字段数据是否存在
 		function nullData(data){
 			if(data == '' || data == "undefined"){
 				return "未定义";
 			}else{
 				return data;
 			}
 		}
 		$(".version").text(nullData(data.version));      //当前版本
		$(".eureka").text(nullData(data.eureka));        //eureka服务器地址
		$(".zuul").text(nullData(data.zuul));    //zuul服务器地址
		$(".zookeeper").text(nullData(data.zookeeper));        //zookeeper服务地址
		$(".elasticsearch").text(nullData(data.elasticsearch));    //elasticsearch地址
		$(".services").text(nullData(data.services));    //服务地址
 	}

})
