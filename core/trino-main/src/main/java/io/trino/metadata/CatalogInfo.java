/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.metadata;

import java.util.HashMap;

/**
 * Created by IntelliJ IDEA
 * TODO: TODO
 *
 * @author: 徐成
 * Date: 2021/11/16
 * Time: 11:07 上午
 * Email: old_camel@163.com
 */


public class CatalogInfo {
    private String catalogName;
    private String connectorName;
    private HashMap<String,String> properties=new HashMap<>();

    @Override
    public String toString() {
        return "CatalogInfo{" +
                "catalogName='" + catalogName + '\'' +
                ", connectorName='" + connectorName + '\'' +
                ", properties=" + properties +
                '}';
    }

    public String getCatalogName()
    {
        return catalogName;
    }
    public void setCatalogName(String catalogName)
    {
        this.catalogName = catalogName;
    }
    public String getConnectorName()
    {
        return connectorName;
    }
    public void setConnectorName(String connectorName)
    {
        this.connectorName = connectorName;
    }
    public HashMap<String, String> getProperties()
    {
        return properties;
    }
    public void setProperties(HashMap<String, String> properties)
    {
        this.properties = properties;
    }
}
