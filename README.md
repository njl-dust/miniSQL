### MINI-SQL

### 构建
#### Windows
目前该代码暂不支持在Windows平台上的编译。但在Win10及以上的系统中，可以通过安装WSL（Windows的Linux子系统）来进行
开发和构建。WSL请选择Ubuntu子系统（推荐Ubuntu20及以上）。如果你使用Clion作为IDE，可以在Clion中配置WSL从而进行调试，具体请参考
[Clion with WSL](https://blog.jetbrains.com/clion/2018/01/clion-and-linux-toolchain-on-windows-are-now-friends/)

#### MacOS & Linux & WSL
基本构建命令
```bash
mkdir build
cd build
cmake ..
make -j
```
若不涉及到`CMakeLists`相关文件的变动且没有新增或删除`.cpp`代码（通俗来说，就是只是对现有代码做了修改）
则无需重新执行`cmake..`命令，直接执行`make -j`编译即可。

默认以`debug`模式进行编译，如果你需要使用`release`模式进行编译：
```bash
cmake -DCMAKE_BUILD_TYPE=Release ..
```

### 测试
在构建后，默认会在`build/test`目录下生成`minisql_test`的可执行文件，通过`./minisql_test`即可运行所有测试。

如果需要运行单个测试，例如，想要运行`lru_replacer_test.cpp`对应的测试文件，可以通过`make lru_replacer_test`
命令进行构建。
