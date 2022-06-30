#!/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys

g_front_end = 'frontend/'
g_back_end = 'backend/'

# 打包single-file-cli:
# 1. 使用pkg
#  pkg -t node14-win-amd64 -c package.json -o single-file --out-path=bin --no-bytecode --public-packages "*" --public --compress=GZip
#  不能交叉编译，可禁用 bytecode 解决
# package.json添加以下字段：
# "pkg": {
# 	"scripts": [ "./back-ends/**/*.js", "./lib/**/*.js" ],
# 	"assets": ""
# }

# 2. 直接将 node.exe 和源代码一起打包, 体积大，文件多

def run_cmd(cmd,wd):
    pass
    
def build_front_end():
    global g_front_end
    pass
    
def build_back_end():
    global g_back_end
    pass

if __name__ == '__main__':
    build_back_end()
    build_front_end()
    
    print('build success')
    