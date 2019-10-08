# 1. 创建德运商品表
def create_devon_product(table_name):
    sql_create_product = """
    CREATE TABLE `{}` (
      `id` int (20) NOT NULL AUTO_INCREMENT COMMENT '索引',
      `num_iid` varchar(32) DEFAULT NULL COMMENT 'ID',
      `product_title` varchar(150) DEFAULT NULL COMMENT '产品名称',
      `product_name` varchar(150) DEFAULT NULL COMMENT '产品类型',
      `num` int(10) DEFAULT NULL COMMENT '商品数量',
      `amount` int(10) DEFAULT NULL COMMENT '小单件数量',
      `unit` varchar(32) DEFAULT NULL COMMENT '单位',
      PRIMARY KEY (`id`),
      UNIQUE KEY `num_iid` (`num_iid`)
        ) ENGINE=InnoDB  DEFAULT CHARSET=utf8 COMMENT='美素产品信息表';
    """.format( table_name)
    return sql_create_product

# 获取美素佳儿的产品表信息
def create_friso_product(table_name):
    sql_create_product = """
    CREATE TABLE `{}` (
      `id` int (20) NOT NULL AUTO_INCREMENT COMMENT '索引',
      `outer_iid` varchar(64) DEFAULT NULL COMMENT '商品编码',
      `title` varchar(150) DEFAULT NULL COMMENT '商品标题',
      `num_iid` varchar(32) DEFAULT NULL COMMENT '',     
      `is_gold` varchar(10) DEFAULT NULL COMMENT '金装，皇家，其他',
      `rank` varchar(10) DEFAULT NULL COMMENT '段位',
      `description` varchar(20) DEFAULT NULL COMMENT '其他说明',
      `is_can` varchar(10) DEFAULT NULL COMMENT '是否罐装',
      `can_num` int(10) DEFAULT NULL COMMENT '罐数/盒数',
      `can_weight` int(10) DEFAULT NULL COMMENT '每罐克数（规格）',
      `send_weight` int(10) DEFAULT NULL COMMENT '加送克数',
      `weight` int(10) DEFAULT NULL COMMENT '重量（罐克数*罐数）',
      `all_weight` int(10) DEFAULT NULL COMMENT '总重量：重量+加送克数',
      `add_time` datetime DEFAULT NULL COMMENT '整理数据的时间',
      PRIMARY KEY (`id`),
      KEY `connect` (`title`, `outer_iid`)
        ) ENGINE=InnoDB  DEFAULT CHARSET=utf8 COMMENT='美素产品信息表';
    """.format(table_name)
    return sql_create_product


# 3. 创建伊利商品表
def create_erie_product(table_name):
    sql_create_product = """
    CREATE TABLE `{}` (
        `id` int (20) NOT NULL AUTO_INCREMENT COMMENT '索引/序号',
        `product_sku_id` varchar (70) DEFAULT NULL COMMENT '产品SKU编号',
        `outer_iid` varchar (20) DEFAULT NULL COMMENT '物品编码',
        `num_iid` varchar (30) DEFAULT NULL COMMENT 'ID',
        `type` varchar (20) DEFAULT NULL COMMENT '系列',
        `rank` varchar (30) DEFAULT NULL COMMENT '段位',
        `num` varchar (10) DEFAULT NULL COMMENT '数量',
        `weight` varchar (20) DEFAULT NULL COMMENT '规格',
        `short_name` varchar (40) DEFAULT NULL COMMENT '名称',
        `combine_name` varchar (60) DEFAULT NULL COMMENT '合成标题',
        `add_time` datetime DEFAULT NULL COMMENT '整理数据的时间',
      PRIMARY KEY (`id`)
      ) ENGINE=InnoDB  DEFAULT CHARSET=utf8 COMMENT='伊利产品表';
    """.format(table_name)
    return sql_create_product


# 1. 创建aldi商品表
def create_aldi_product(table_name):
    sql_create_product = """
    CREATE TABLE `{}` (
        `id` int (20) NOT NULL AUTO_INCREMENT COMMENT '索引/序号',
        `category` varchar (20) DEFAULT NULL COMMENT '分类专区',
        `num_iid` varchar (30) DEFAULT NULL COMMENT '商品ID',
        `english` varchar (20) DEFAULT NULL COMMENT '分类英文名',
        `title` varchar (60) DEFAULT NULL COMMENT '标题',
        `num` varchar (10) DEFAULT NULL COMMENT '数量',
        `add_time` datetime DEFAULT NULL COMMENT '添加时间',
      PRIMARY KEY (`id`)
      ) ENGINE=InnoDB  DEFAULT CHARSET=utf8 COMMENT='aldi产品表';
    """.format( table_name)
    return sql_create_product

# 1. 创建gant商品表
def create_gant_product(table_name):
    sql_create_product = """
    CREATE TABLE `{}` (
      `id` int (20) NOT NULL AUTO_INCREMENT COMMENT '索引',
      `num_iid` varchar(32) DEFAULT NULL COMMENT '商品ID',     
      `outer_iid` varchar(64) DEFAULT NULL COMMENT '商品编码',
      `outer_iid2` varchar(64) DEFAULT NULL COMMENT '商品编码',
      `outer_sku_id` varchar(50) DEFAULT NULL COMMENT '单品id',
      `sku_properties_name` VARCHAR (40) DEFAULT NULL COMMENT '商品口味',
      `title` varchar(150) DEFAULT NULL COMMENT '商品标题',
      `type` varchar(20) DEFAULT NULL COMMENT '品类',
      `category` varchar(20) DEFAULT NULL COMMENT '商品大类',
      `gender` varchar(20) DEFAULT NULL COMMENT '男装/女装',
      `season` VARCHAR(20) DEFAULT NULL COMMENT '上市季节',
      `add_time` datetime DEFAULT NULL COMMENT '整理数据的时间',
      PRIMARY KEY (`id`),
      key `num_title`(`outer_iid`)
        ) ENGINE=InnoDB  DEFAULT CHARSET=utf8 COMMENT='gant商品表';
    """.format( table_name)
    return sql_create_product


# 1. 创建家乐氏商品表
def create_kelloggs_product(table_name):
    sql_create_product = """
    CREATE TABLE `{}` (
      `id` int (20) NOT NULL AUTO_INCREMENT COMMENT '索引',
      `num_iid` varchar(32) DEFAULT NULL COMMENT '商品ID',     
      `outer_sku_id` varchar(32) DEFAULT NULL COMMENT '',
      `outer_iid` varchar(64) DEFAULT NULL COMMENT '商品编码',
      `title` varchar(150) DEFAULT NULL COMMENT '商品标题',
      `sku_properties_name` VARCHAR (30) DEFAULT NULL COMMENT '商品口味',
      `description` varchar(20) DEFAULT NULL COMMENT '其他说明',
      `min_created` datetime DEFAULT NULL COMMENT '最小创建日期',
      `add_time` datetime DEFAULT NULL COMMENT '整理数据的时间',
      PRIMARY KEY (`id`),
      key `num_title`(`num_iid`,`title`)
        ) ENGINE=InnoDB  DEFAULT CHARSET=utf8 COMMENT='家乐氏食品产品表';
    """.format( table_name)
    return sql_create_product


# 1. 创建好侍商品表
def create_house_product(table_name):
    sql_create_product = """
    CREATE TABLE `{}` (
      `id` int (20) NOT NULL AUTO_INCREMENT COMMENT '索引',
      `outer_iid` varchar(64) DEFAULT NULL COMMENT '商品编码',
      `title` varchar(150) DEFAULT NULL COMMENT '商品标题',
      `num_iid` varchar(32) DEFAULT NULL COMMENT '',     
      `type` VARCHAR (20) DEFAULT NULL COMMENT '商品分类',
      `taste` VARCHAR (25) DEFAULT NULL COMMENT '辣味分类',
      `description` varchar(20) DEFAULT NULL COMMENT '其他说明',
      `add_time` datetime DEFAULT NULL COMMENT '整理数据的时间',
      PRIMARY KEY (`id`),
      key `type`(`type`)
        ) ENGINE=InnoDB  DEFAULT CHARSET=utf8 COMMENT='好侍食品产品表';
    """.format(table_name)
    return sql_create_product

