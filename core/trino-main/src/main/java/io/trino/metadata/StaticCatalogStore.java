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

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Files;
import io.airlift.discovery.client.Announcer;
import io.airlift.discovery.client.ServiceAnnouncement;
import io.airlift.log.Logger;
import io.trino.connector.ConnectorManager;

import javax.crypto.Cipher;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import javax.inject.Inject;
import javax.validation.constraints.NotNull;

import io.trino.execution.scheduler.NodeSchedulerConfig;
import okhttp3.MediaType;
import okhttp3.Headers;
import okhttp3.HttpUrl;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.spec.AlgorithmParameterSpec;

import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.nullToEmpty;
import static io.airlift.configuration.ConfigurationLoader.loadPropertiesFrom;
import static io.airlift.discovery.client.ServiceAnnouncement.serviceAnnouncement;
import static java.util.Objects.requireNonNull;

public class StaticCatalogStore {
    private static final Logger log = Logger.get(StaticCatalogStore.class);
    private final ConnectorManager connectorManager;
    private final File catalogConfigurationDir;
    private final Set<String> disabledCatalogs;
    private final AtomicBoolean catalogsLoading = new AtomicBoolean();
    private final AtomicBoolean catalogsLoaded = new AtomicBoolean();
    private Announcer announcer;
    private Map<String, CatalogInfo> catalogMap = new HashMap<>();
    private static final String AES_NAME = "AES";
    private static final String ALGORITHM = "AES/CBC/PKCS5Padding";
    private static final String KEY = "2021A2C88F9F8D1D";
    private static final String IV = "2021038200F44941";



    @Inject
    public StaticCatalogStore(ConnectorManager connectorManager, StaticCatalogStoreConfig config, Announcer announcer)
    {
        this(connectorManager,
                config.getCatalogConfigurationDir(),
                firstNonNull(config.getDisabledCatalogs(), ImmutableList.of()), announcer);
    }

    public StaticCatalogStore(ConnectorManager connectorManager, File catalogConfigurationDir, List<String> disabledCatalogs, Announcer announcer)
    {

        this.connectorManager = connectorManager;
        this.catalogConfigurationDir = catalogConfigurationDir;
        this.disabledCatalogs = ImmutableSet.copyOf(disabledCatalogs);
        this.announcer = requireNonNull(announcer, "announcer is null");

    }

    public void loadCatalogs() throws Exception
    {
        log.info("start load catalogs");

        if (!catalogsLoading.compareAndSet(false, true))
        {
            log.info("return");

            return;
        }
        catalogsLoaded.set(true);
        Timer timer = new Timer();


        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                log.info("catalogMap:" + catalogMap);
                Map<String, CatalogInfo> newCatalog = new HashMap<>();
                Map<String, CatalogInfo> updateCatalog = new HashMap<>();
                List<String> remoteCatalog = new ArrayList<>();
                Set<String> strings = null;
                try {
                    List<CatalogInfo> catalogs = loadRemoteCatalogs();
                    for (CatalogInfo catalogInfo : catalogs) {
                        String catalogName = catalogInfo.getCatalogName();
                        remoteCatalog.add(catalogName);
                        if (catalogMap.get(catalogName) == null) {
                            //add catalog
                            newCatalog.put(catalogName, catalogInfo);
                        } else if (catalogMap.get(catalogName) != null && !CataLogInfoEquals(catalogMap.get(catalogName), catalogInfo)) {
                            //update catalog
                            updateCatalog.put(catalogName, catalogInfo);
                        }
                    }
                    strings = newCatalog.keySet();
                    log.info("add catalog:" + strings);
                    for (String catalogname : strings) {
                        addCatalog(newCatalog.get(catalogname));
                    }

                    log.info("update catalog:" + updateCatalog);
                    for (String catalogname : updateCatalog.keySet()) {
                        updateCatalog(updateCatalog.get(catalogname));
                    }
                    List<String> deleteCatalog = catalogMap.keySet().stream().filter(x -> !remoteCatalog.contains(x)).collect(Collectors.toList());
                    log.info("delete catalog:" + deleteCatalog);
                    for (String catalogname : deleteCatalog) {
                        deleteCatalog(catalogMap.get(catalogname));
                    }

                    newCatalog.clear();
                    updateCatalog.clear();
                    deleteCatalog.clear();
                } catch (IOException e) {
                    log.error("获取catalog error", e);
                    e.printStackTrace();
                }
            }
        }, 1 , 60000);


    }

    private Boolean CataLogInfoEquals(CatalogInfo old, CatalogInfo now)
    {
        if (!old.getCatalogName().equals(now.getCatalogName())) {
            return false;
        }
        if (!old.getConnectorName().equals(now.getConnectorName())) {
            return false;
        }
        if (old.getProperties().size() != now.getProperties().size()) {
            return false;
        }

        for (String key : old.getProperties().keySet()) {
            if (!now.getProperties().containsKey(key)) {
                return false;
            } else {
                if (!now.getProperties().get(key).equals(old.getProperties().get(key))) {
                    return false;
                }
            }
        }

        for (String key : now.getProperties().keySet()) {
            if (!old.getProperties().containsKey(key)) {
                return false;
            } else {
                if (!old.getProperties().get(key).equals(now.getProperties().get(key))) {
                    return false;
                }
            }
        }

        return true;
    }

    private void deleteCatalog(CatalogInfo catalogInfo) {
        if (catalogInfo != null) {
            try {
                connectorManager.dropConnection(catalogInfo.getCatalogName());
                updateConnectorIds(announcer, catalogInfo.getCatalogName(), CataLogAction.DELETE);
                catalogMap.remove(catalogInfo.getCatalogName());
                log.info("deleteCatalog:" + catalogInfo.toString());
            } catch (Exception e) {
                e.printStackTrace();
                log.info("deleteCatalog:" + catalogInfo.toString() + " is error!");
            }
        }
    }

    private void addCatalog(CatalogInfo catalogInfo) {
        try {
            connectorManager.createCatalog(catalogInfo.getCatalogName(), catalogInfo.getConnectorName(), ImmutableMap.copyOf(catalogInfo.getProperties()));
            updateConnectorIds(announcer, catalogInfo.getCatalogName(), CataLogAction.ADD);
            catalogMap.put(catalogInfo.getCatalogName(), catalogInfo);
            log.info("--------------add or update catalog ------------------" + catalogInfo);
        } catch (Exception e) {
            e.printStackTrace();
            log.info("--------------add or update catalog ------------------" + catalogInfo + " is error!");
        }
    }

    private void updateCatalog(CatalogInfo catalogInfo) {
        deleteCatalog(catalogInfo);
        addCatalog(catalogInfo);
        catalogMap.put(catalogInfo.getCatalogName(), catalogInfo);
    }

    private List<CatalogInfo> loadRemoteCatalogs() throws IOException {
        Map<String, String> properties = new HashMap<>(loadPropertiesFrom("etc/custom.properties"));
        String url = properties.get("catalogs.uri");
        MediaType mediaType = MediaType.parse("application/json; charset=utf-8");
        Map<String, String> requst = new HashMap<String, String>();
        requst.put("aesData", encrypt("{}", KEY));
        RequestBody body = RequestBody.create(mediaType, JSON.toJSONString(requst));
        OkHttpClient client = new OkHttpClient();
        Request.Builder builder = new Request.Builder();
        Request request = builder.url(url).post(body).build();
        HttpUrl.Builder urlBuilder = request.url().newBuilder();
        Headers.Builder headerBuilder = request.headers().newBuilder();
        builder.url(urlBuilder.build()).headers(headerBuilder.build());
        Response execute = client.newCall(builder.build()).execute();
        if (execute.isSuccessful()) {
            try {
                String json = execute.body().string();
                Map<String, String> map = JSON.parseObject(json, new TypeReference<Map<String, String>>() {
                });
                String decrypt = decrypt(map.get("data"), KEY);
                List<Map<String, String>> maps = JSON.parseObject(decrypt, new TypeReference<List<Map<String, String>>>() {
                });
                log.info("the catalog obtained from the interface is :" + maps);
                List<CatalogInfo> catalogs = new ArrayList<>();
                for (Map<String, String> catalogmap : maps) {
                    CatalogInfo catalogInfo = new CatalogInfo();
                    catalogInfo.setCatalogName(catalogmap.get("connection-name"));
                    catalogInfo.setConnectorName(catalogmap.get("connection-type"));
                    for (String key : catalogmap.keySet()) {
                        if (!"connection-name".equals(key) && !"connection-type".equals(key)) {
                            catalogInfo.getProperties().put(key, catalogmap.get(key));
                        }
                    }
                    catalogs.add(catalogInfo);
                }
                return catalogs;
            } catch (Exception e) {
                e.printStackTrace();
                log.error(e);
            }
            return new ArrayList<CatalogInfo>();
        } else {
            log.info(execute.toString());
            return new ArrayList<CatalogInfo>();
        }
    }


    public static String decrypt(@NotNull String content, @NotNull String key) {
        try {
            Cipher cipher = Cipher.getInstance(ALGORITHM);
            SecretKeySpec keySpec = new SecretKeySpec(key.getBytes(StandardCharsets.UTF_8), AES_NAME);
            AlgorithmParameterSpec paramSpec = new IvParameterSpec(IV.getBytes(StandardCharsets.UTF_8));
            cipher.init(Cipher.DECRYPT_MODE, keySpec, paramSpec);
            return new String(cipher.doFinal(requireNonNull(Base64.getDecoder().decode(content))), StandardCharsets.UTF_8);
        } catch (Exception ex) {
            log.error("数据解密异常: " + content);
            return null;
        }
    }

    public static String encrypt(@NotNull String content, @NotNull String key) {
        try {
            Cipher cipher = Cipher.getInstance(ALGORITHM);
            SecretKeySpec keySpec = new SecretKeySpec(key.getBytes(StandardCharsets.UTF_8), AES_NAME);
            AlgorithmParameterSpec paramSpec = new IvParameterSpec(IV.getBytes(StandardCharsets.UTF_8));
            cipher.init(Cipher.ENCRYPT_MODE, keySpec, paramSpec);
            return Base64.getEncoder().encodeToString(cipher.doFinal(content.getBytes(StandardCharsets.UTF_8)));
        } catch (Exception ex) {
            log.error("请求参数加密异常: " + content);
            return null;
        }
    }

    private void loadCatalog(File file) throws Exception {
        //***如果想支持从client端配置，需要添加规则，因为无法识别配置文件中的数据源类型***

        String catalogName = Files.getNameWithoutExtension(file.getName());
        if (disabledCatalogs.contains(catalogName)) {
            log.info("Skipping disabled catalog %s", catalogName);
            return;
        }

        log.info("-- Loading catalog %s --", file);
        Map<String, String> properties = new HashMap<>(loadPropertiesFrom(file.getPath()));

        String connectorName = properties.remove("connector.name");

        CatalogInfo catalogInfo = new CatalogInfo();
        catalogInfo.setCatalogName(properties.get("connection-url"));

        checkState(connectorName != null, "Catalog configuration %s does not contain connector.name", file.getAbsoluteFile());

//        connectorManager.createCatalog(catalogName, connectorName, ImmutableMap.copyOf(properties));
        log.info("-- Added catalog %s using connector %s --", catalogName, connectorName);
    }


    private static List<File> listFiles(File installedPluginsDir) {
        if (installedPluginsDir != null && installedPluginsDir.isDirectory()) {
            File[] files = installedPluginsDir.listFiles();
            if (files != null) {
                return ImmutableList.copyOf(files);
            }
        }
        return ImmutableList.of();
    }

    private void updateConnectorIds(Announcer announcer, String catalogName, CataLogAction action) {
        ServiceAnnouncement announcement = getTrinoAnnouncement(announcer.getServiceAnnouncements());
        String property = nullToEmpty(announcement.getProperties().get("connectorIds"));
        List<String> values = Splitter.on(',').trimResults().omitEmptyStrings().splitToList(property);
        Set<String> connectorIds = new LinkedHashSet<>(values);
        if (CataLogAction.DELETE.equals(action)) {
            connectorIds.remove(catalogName);
        } else {
            connectorIds.add(catalogName);
        }

        ServiceAnnouncement.ServiceAnnouncementBuilder builder = serviceAnnouncement(announcement.getType());
        for (Map.Entry<String, String> entry : announcement.getProperties().entrySet()) {
            if (!entry.getKey().equals("connectorIds")) {
                builder.addProperty(entry.getKey(), entry.getValue());
            }
        }
        builder.addProperty("connectorIds", Joiner.on(',').join(connectorIds));
        announcer.removeServiceAnnouncement(announcement.getId());
        announcer.addServiceAnnouncement(builder.build());
    }

    private static ServiceAnnouncement getTrinoAnnouncement(Set<ServiceAnnouncement> announcements) {
        for (ServiceAnnouncement announcement : announcements) {
            if (announcement.getType().equals("trino")) {
                return announcement;
            }
        }
        throw new IllegalArgumentException("Trino announcement not found: " + announcements);
    }

    public enum CataLogAction {
        ADD, DELETE, UPDATE
    }

}
