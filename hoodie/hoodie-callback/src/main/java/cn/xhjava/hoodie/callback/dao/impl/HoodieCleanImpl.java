package cn.xhjava.hoodie.callback.dao.impl;

import cn.xhjava.hoodie.callback.dao.HoodieClean;
import cn.xhjava.hoodie.callback.domain.ApplicationConf;
import cn.xhjava.hoodie.callback.domain.HoodieCallbackMsg;
import cn.xhjava.hoodie.callback.util.SSHClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * @author Xiahu
 * @create 2021-06-18
 */
@Service
public class HoodieCleanImpl implements HoodieClean {

    @Autowired
    private ApplicationConf conf;

    private SSHClient sshClient;

    @Override
    public void clean(HoodieCallbackMsg msg) {
        sshClient = new SSHClient(conf.getSshShellHost(), conf.getSshShellPort(), conf.getSshShellUser(), conf.getSshShellPass());
    }
}
