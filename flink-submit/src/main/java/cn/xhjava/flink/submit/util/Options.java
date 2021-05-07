/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cn.xhjava.flink.submit.util;

import org.apache.commons.lang.StringUtils;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.yarn.configuration.YarnConfigOptions;


/**
 * This class define commandline options for the Launcher program
 *
 * Company: www.dtstack.com
 * @author huyifan.zju@163.com
 */
public class Options {


    private String job;

    private String monitor;

    private String jobid = "Flink Job";

    private String flinkconf;

    private String pluginRoot;

    private String yarnconf;

    private String parallelism = "1";

    private String priority = "1";

    private String queue = "default";

    private String flinkLibJar;

    private String confProp = "{}";

    private String p = "";

    private String s;

    private String pluginLoadMode = "shipfile";

    private String krb5conf ;

    private String keytab ;

    private String principal ;

    private String appId;

    private String remotePluginPath;

    private Configuration flinkConfiguration = null;

    public String getAppId() {
        return appId;
    }

    public void setAppId(String appId) {
        this.appId = appId;
    }

    public String getS() {
        return s;
    }

    public void setS(String s) {
        this.s = s;
    }

    public String getConfProp() {
        return confProp;
    }

    public void setConfProp(String confProp) {
        this.confProp = confProp;
    }

    public String getParallelism() {
        return parallelism;
    }

    public void setParallelism(String parallelism) {
        this.parallelism = parallelism;
    }

    public String getJob() {
        return job;
    }

    public void setJob(String job) {
        this.job = job;
    }

    public String getMonitor() {
        return monitor;
    }

    public void setMonitor(String monitor) {
        this.monitor = monitor;
    }

    public String getJobid() {
        return jobid;
    }

    public void setJobid(String jobid) {
        this.jobid = jobid;
    }

    public String getFlinkconf() {
        return flinkconf;
    }

    public void setFlinkconf(String flinkconf) {
        this.flinkconf = flinkconf;
    }

    public String getPluginRoot() {
        return pluginRoot;
    }

    public void setPluginRoot(String pluginRoot) {
        this.pluginRoot = pluginRoot;
    }

    public String getYarnconf() {
        return yarnconf;
    }

    public void setYarnconf(String yarnconf) {
        this.yarnconf = yarnconf;
    }

    public String getPriority() {
        return priority;
    }

    public void setPriority(String priority) {
        this.priority = priority;
    }

    public String getQueue() {
        return queue;
    }

    public void setQueue(String queue) {
        this.queue = queue;
    }

    public String getFlinkLibJar() {
        return flinkLibJar;
    }

    public void setFlinkLibJar(String flinkLibJar) {
        this.flinkLibJar = flinkLibJar;
    }

    public String getPluginLoadMode() {
        return pluginLoadMode;
    }

    public void setPluginLoadMode(String pluginLoadMode) {
        this.pluginLoadMode = pluginLoadMode;
    }

    public String getRemotePluginPath() {
        return remotePluginPath;
    }

    public void setRemotePluginPath(String remotePluginPath) {
        this.remotePluginPath = remotePluginPath;
    }

    public String getP() {
        return p;
    }

    public void setP(String p) {
        this.p = p;
    }

    public String getKrb5conf() {
        return krb5conf;
    }

    public void setKrb5conf(String krb5conf) {
        this.krb5conf = krb5conf;
    }

    public String getKeytab() {
        return keytab;
    }

    public void setKeytab(String keytab) {
        this.keytab = keytab;
    }

    public String getPrincipal() {
        return principal;
    }

    public void setPrincipal(String principal) {
        this.principal = principal;
    }

    @Override
    public String toString() {
        return "Options{" +
                ", job='" + job + '\'' +
                ", monitor='" + monitor + '\'' +
                ", jobid='" + jobid + '\'' +
                ", flinkconf='" + flinkconf + '\'' +
                ", pluginRoot='" + pluginRoot + '\'' +
                ", yarnconf='" + yarnconf + '\'' +
                ", parallelism='" + parallelism + '\'' +
                ", priority='" + priority + '\'' +
                ", queue='" + queue + '\'' +
                ", flinkLibJar='" + flinkLibJar + '\'' +
                ", confProp='" + confProp + '\'' +
                ", p='" + p + '\'' +
                ", s='" + s + '\'' +
                ", pluginLoadMode='" + pluginLoadMode + '\'' +
                ", appId='" + appId + '\'' +
                ", remotePluginPath='" + remotePluginPath + '\'' +
                ", krb5conf='" + krb5conf + '\'' +
                ", keytab='" + keytab + '\'' +
                ", principal='" + principal + '\'' +
                '}';
    }
}
