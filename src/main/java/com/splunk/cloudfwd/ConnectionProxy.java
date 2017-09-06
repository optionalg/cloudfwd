package com.splunk.cloudfwd;

import java.io.Closeable;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Properties;

/**
 * Created by eprokop on 9/6/17.
 */
public class ConnectionProxy implements InvocationHandler {
    private Connection activeConnection;
    private ConnectionCallbacks callbacks;

    private ConnectionProxy(Connection activeConnection, ConnectionCallbacks callbacks) {
        this.activeConnection = activeConnection;
        this.callbacks = callbacks;
    }

    public static IConnection newInstance(ConnectionCallbacks callbacks) {
        Connection c = new Connection(callbacks);
        ConnectionProxy handler = new ConnectionProxy(c, callbacks);
        return (IConnection) Proxy.newProxyInstance(c.getClass().getClassLoader(),
                new Class<?>[] { IConnection.class, Closeable.class }, handler);
    }

    public static IConnection newInstance(ConnectionCallbacks callbacks, Properties settings) {
        Connection c = new Connection(callbacks, settings);
        ConnectionProxy handler = new ConnectionProxy(c, callbacks);
        return (IConnection) Proxy.newProxyInstance(c.getClass().getClassLoader(),
                new Class<?>[]{ IConnection.class, Closeable.class }, handler);
    }

    private void setActiveConnection(Connection activeConnection) {
        this.activeConnection = activeConnection;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args)
            throws Throwable {
        if (method.getName().equals("setProperties")) {
            // TODO: some checks here to make sure that it has arguments
            activeConnection.close();
            Properties newSettings = (Properties)args[0];
            setActiveConnection(new Connection(callbacks, newSettings));
            return null; // TODO: do something else?
        } else {
            try {
                return method.invoke(activeConnection, args);
            } catch(InvocationTargetException e) {
                throw e.getTargetException();
            } catch(IllegalArgumentException|IllegalAccessException e) {
                throw e;
            }
        }
    }
}
