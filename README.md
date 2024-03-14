# 依赖导入
经过本人测试，由于设置了go mod的代理，导致旧版本的代码会被导入，所以需要设置环境变量$env:GONOSUMDB="github.com/bing-bing-student/redis-mq"
其作用是导入依赖时不需要验证，然后再使用go mod tidy导入依赖即可。

# 基本使用
```

```