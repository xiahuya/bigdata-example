package com.clb.hive.auth;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.service.auth.PasswdAuthenticationProvider;
import org.slf4j.Logger;

import javax.security.sasl.AuthenticationException;

/**
 * @author Xiahu
 * @create 2022/7/27 0027
 */
public class CustomPasswdAuthenticator implements PasswdAuthenticationProvider {

    private Logger LOG = org.slf4j.LoggerFactory.getLogger(CustomPasswdAuthenticator.class);

    private static final String HIVE_JDBC_PASSWD_AUTH_PREFIX = "hive.jdbc_passwd.auth.%s";

    private Configuration conf = null;

    @Override
    public void Authenticate(String userName, String passwd) throws AuthenticationException {
        LOG.info("user: {} try login.", userName);

        String passwdConf = getConf().get(String.format(HIVE_JDBC_PASSWD_AUTH_PREFIX, userName));
        if (passwdConf == null) {
            String message = "user's ACL configration is not found. user:" + userName;
            LOG.info(message);
            throw new AuthenticationException(message);
        }
        if (!passwd.equals(passwdConf)) {
            String message = "user name and password is mismatch. user:" + userName;
            throw new AuthenticationException(message);
        }
    }

    public Configuration getConf() {
        if (conf == null) {
            this.conf = new Configuration(new HiveConf());
        }
        return conf;
    }

    public void setConf(Configuration conf) {
        this.conf = conf;
    }


}
