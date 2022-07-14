# offliner

## 后端服务

用 golang 启动 HTTP 服务，该服务可以：

- 解析短链接返回保存的网页
- 提供管理页面（该服务交互全部由 WEB 承载）
- 开机自启动
- 浏览器插件提取页面后将文件上传到后端
- 在桌面创建快捷方式，用于打开管理页面（或者以 webview 应用的形式）

## 交互

前后端分离，前端使用 angular(typescript) 制作静态页面

## 多端数据同步

不依赖中心化的数据同步服务，要同步数据可以将数据目录设置到同步盘

### 选型

storage/readme.md

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

https://github.com/puppeteer/puppeteer
- *.html + *_files 形式?
- 不会下载全部资源, 有些资源依然是url

https://github.com/gildas-lormeau/SingleFile
- 浏览器插件, 支持大多数浏览器
- 单页面保存
- 支持保存选定内容、选定frame
- 批量保存
- 注解、修改保存的网页
- 可保存至Google Drive，GitHub
- [已知问题列表](https://github.com/gildas-lormeau/SingleFile#known-issues)，看了下都是可接受的小问题
- 仍在维护，使用的项目也比较多

https://github.com/gildas-lormeau/single-file-cli
- SingleFile的命令行版本
- 单页面保存
- 需要docker, 或许能够可以脱离运行
- 脱离运行需要nodejs, 以及 Chrome 或 Firefox 或者jsdom
- 还在维护

https://github.com/zTrix/webpage2html
- 需要python
- 单页面保存
- 直接保存有无权限问题
- 先用浏览器保存, 再压缩成单文件的问题是: 浏览器不会下载某些资源
- 不支持less
- 不支持srcset
- [SATA协议](https://github.com/zTrix/webpage2html/blob/master/LICENSE.txt)
- 很久没维护了

https://github.com/markusmobius/nodeSavePageWE
- 依赖nodejs
- 单页面保存
- 从 Chrome SavePageWE 扩展改造, 可靠性不错
- 存在问题, 尚不确知
- 很久没维护了

综合对比，直接保存或借助浏览器保存，https://github.com/gildas-lormeau/single-file-cli都是最优选择

## 易用性

提出如下能提高软件易用性的建议：

- 可以配置自定义域名，通过修改 DNS 实现？
    - 修改 hosts 文件或者运行 DNS 服务会让用户感觉不安全，并且需要高权限
      - 我们无法监听 80 端口，访问时还是需要在域名后面指定端口，没有想象中简洁
    - 自定义协议跳转到 offliner 然后跳转到浏览器过于繁琐，而且自定义协议的链接一般无法被识别为链接
    - 最好直接拷贝 a 标签，实际链接为 localhost:port/abc 形式
- URL前添加 offline/ 快捷保存
- 支持批量保存
- 以后阅读和提醒支持
- 增加迁移功能（机器间，如机器不再使用）
- 增加导出功能（如果不再使用offliner）
  
## 可靠性

保持几份数据文件备份

## 相似项目

- https://github.com/ArchiveBox/ArchiveBox
    - 没有多端同步支持
    - 没有浏览器插件保存授权内容
    - 过于复杂

- https://github.com/pjamar/htmls-to-datasette
    - 功能比较单一
  