package com.luodifz.vo;

import java.io.Serializable;

/**
 * Created by liujinjing on 2017/7/11.
 */
public abstract class AbstractObjectMessage implements Serializable{
    private String key;

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }
}
