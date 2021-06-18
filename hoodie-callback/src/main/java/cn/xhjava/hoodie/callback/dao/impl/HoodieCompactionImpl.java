package cn.xhjava.hoodie.callback.dao.impl;

import cn.xhjava.hoodie.callback.dao.HoodieCompaction;
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
public class HoodieCompactionImpl implements HoodieCompaction {

    @Autowired
    private ApplicationConf conf;

    private SSHClient sshClient;
    

    @Override
    public void compact(HoodieCallbackMsg msg) {
        sshClient = new SSHClient(conf.getSshShellHost(), conf.getSshShellPort(), conf.getSshShellUser(), conf.getSshShellPass());
    }
}
