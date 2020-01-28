package org.apache.hadoop.util;

import org.apache.hadoop.metrics2.util.MBeans;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.ObjectName;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;

public class JmxUtil {
    private static final Logger LOG = LoggerFactory.getLogger(JmxUtil.class);

    /**
     * Register the provided MBean with additional JMX ObjectName properties.
     * If additional properties are not supported then fallback to registering
     * without properties.
     *
     * @param serviceName - see {@link MBeans#register}
     * @param mBeanName - see {@link MBeans#register}
     * @param jmxProperties - additional JMX ObjectName properties.
     * @param mBean - the MBean to register.
     * @return the named used to register the MBean.
     */
    public static ObjectName registerWithJmxProperties(
            String serviceName, String mBeanName, Map<String, String> jmxProperties,
            Object mBean) {
        try {

            // Check support for registering with additional properties.
            final Method registerMethod = MBeans.class.getMethod(
                    "register", String.class, String.class,
                    Map.class, Object.class);

            return (ObjectName) registerMethod.invoke(
                    null, serviceName, mBeanName, jmxProperties, mBean);

        } catch (NoSuchMethodException | IllegalAccessException |
                InvocationTargetException e) {

            // Fallback
            if (LOG.isTraceEnabled()) {
                LOG.trace("Registering MBean {} without additional properties {}",
                        mBeanName, jmxProperties);
            }
            return MBeans.register(serviceName, mBeanName, mBean);
        }
    }
}
