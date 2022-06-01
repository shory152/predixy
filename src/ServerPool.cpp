/*
 * predixy - A high performance and full features proxy for redis.
 * Copyright (C) 2017 Joyield, Inc. <joyield.com@gmail.com>
 * All rights reserved.
 */

#include "Server.h"
#include "ServerPool.h"
#include "Handler.h"

ServerPool::~ServerPool()
{
}

void ServerPool::init(const ServerPoolConf& conf)
{
    mPassword = conf.password;
    mMasterReadPriority = conf.masterReadPriority;
    mStaticSlaveReadPriority = conf.staticSlaveReadPriority;
    mDynamicSlaveReadPriority = conf.dynamicSlaveReadPriority;
    mRefreshInterval = conf.refreshInterval;
    mServerTimeout = conf.serverTimeout;
    mServerFailureLimit = conf.serverFailureLimit;
    mServerRetryTimeout = conf.serverRetryTimeout;
    mKeepAlive = conf.keepalive;
    mDbNum = conf.databases;
}

bool ServerPool::refresh()
{
    long last = mLastRefreshTime;
    long now = Util::nowUSec();
    if (now - last < mRefreshInterval) {
        return false;
    }
    return AtomicCAS(mLastRefreshTime, last, now);
}

void ServerPool::handleResponse(Handler* h, ConnectConnection* s, Request* req, Response* res)
{
    UniqueLock<Mutex> lck(mMtx, TryLockTag);
    if (!lck) {
        logNotice("server pool is updating by other thread");
        return;
    }
    mHandleResponseFunc(this, h, s, req, res);
}

Server* ServerPool::randServer(Handler* h, const std::vector<Server*>& servs)
{
    Server* s = nullptr;
    int idx = h->rand() % servs.size();
    for (size_t i = 0; i < servs.size(); ++i) {
        Server* serv = servs[idx++ % servs.size()];
        if (!serv->online()) {
            continue;
        } else if (serv->fail()) {
            s = serv;
        } else {
            return serv;
        }
    }
    return s;
}

Server* ServerPool::iter(const std::vector<Server*>& servs, int& cursor)
{
    int size = servs.size();
    if (cursor < 0 || cursor >= size) {
        return nullptr;
    }
    while (cursor < size) {
        Server* serv = servs[cursor++];
        if (serv->online()) {
            return serv;
        }
    }
    return nullptr;
}

void ServerPool::freeFailureServers()
{
    static time_t last_show = time(NULL);
    if (difftime(time(NULL), last_show) > 60) {
        logWarn("invalid servers: %d", mInvalidServs.size());
        last_show = time(NULL);
    }

    return; // don't free servers at present

    int lastI = mInvalidServs.size() - 1;
    for (int i=lastI; i>=0; --i) {
        auto s = mInvalidServs[i];

        if (difftime(time(NULL), s.time) > 300) {
            logWarn("free %s server %s", s.s->isMaster()? "master" : "slave", s.s->addr().data());
            delete s.s;
            if (i < lastI) {
                mInvalidServs[i] = mInvalidServs[lastI];
            }
            lastI--;
        }
    }
    if (lastI < mInvalidServs.size()-1) {
        mInvalidServs.resize(lastI+1);
    }
}