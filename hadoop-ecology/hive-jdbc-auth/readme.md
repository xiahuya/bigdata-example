## 使用
https://blog.csdn.net/yamaxifeng_132/article/details/119869080


beeline -u jdbc:hive2://master:10000  -n hive -p hive_169

```xml
<property>
    <name>hive.server2.authentication</name>
    <value>CUSTOM</value>
</property>
<property>
    <name>hive.server2.custom.authentication.class</name>
    <value>com.clb.hive.auth.CustomPasswdAuthenticator</value>
</property>
<property>
    <name>hive.jdbc_passwd.auth.hive</name>
    <value>hive_169</value>
</property>
```



