layui.config({
	base : "js/"
}).use(['form','layer','jquery','table'],function(){
	var form = layui.form,
		layer = parent.layer === undefined ? layui.layer : parent.layer,
		table = layui.table,
		$ = layui.jquery;

	table.render({
        elem: '#schema_list'
        ,url:'/schemas/list'
		,where:{
        	name: ''
		}
        ,cellMinWidth: 80
        ,cols: [[
            {type:'checkbox'}
            ,{field:'name', title: '名称'}
            ,{field:'source_code', title: '源数据表前缀'}
            ,{field:'std_code',title: '对应标准数据表'}
            ,{field:'creator', title: '创建人'}
            ,{field:'add_time', title: '创建时间', sort: true}
            ,{field:'upd_time', title: '修改时间', sort: true}
            ,{field:'descp', title: '简介'}
        ]]
        ,page: true
        ,id: 'schemas'
	});

    var active = {
    	reload: function(){
            var name = $("#name").val();
    		//执行重载
            table.reload('schemas', {
                page: {
                    curr: 1 //重新从第 1 页开始
                }
                ,where: {
                    name: name
                }
            });
        }
        ,getCheckData: function(){ //获取选中数据
            var checkStatus = table.checkStatus('schemas')
                ,data = checkStatus.data;
            layer.alert(JSON.stringify(data));
        }
    };

    $('.dataTable .layui-btn').on('click', function(){
        var type = $(this).data('type');
        active[type] ? active[type].call(this) : '';
    });
})
