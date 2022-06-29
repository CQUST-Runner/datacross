# offliner

## 后端服务

用 golang 启动 HTTP 服务，该服务可以：

- 解析短链接返回保存的网页
- 提供配置页面（该服务交互全部由 WEB 承载）
- 开机自启动

## 交互

前后端分离，前端使用 angular(typescript) 制作静态页面

## 多端数据同步

不依赖中心化的数据同步服务，要同步数据可以将数据目录设置到同步盘

### 选型

用 sqlite 持久化数据，用日志跟踪最近操作

- 日志让多机可以并发操作不同的key。但同一个key，要确保操作的先后顺序，由此需要解决多机的时间同步问题（以用户最后一次操作为准，但还是可能有脏写）
- 组合 sqlite 查询（持久数据）和在内存中维护易于查询的数据结构（近期数据）

### 数据项

要保存的数据项有：

``` txt
文件数据：
    单文件网页 -- 只读文件，无同步问题
    
sqlite 数据：
    网页源地址
    离线访问用的短网址
    单文件网页文件名
```

## 网页保存

保存网页的技术要求

- 需要支持从浏览器插件保存，因为有些需要授权的内容 offliner 自身无法访问
- 可选保存为PDF，或者单文件HTML，提高整体可用性
- 保存的网页文件需要禁用掉查看时访问网络的能力
- 给每个保存的网页添加顶层快捷按钮，可直接修改、删除等，也可跳转至配置页

### 保存方案调研
https://github.com/gildas-lormeau/single-file-cli -- 需要docker

## 易用性

提出如下能提高软件易用性的建议：

- 可以配置自定义域名，通过修改 DNS 实现？