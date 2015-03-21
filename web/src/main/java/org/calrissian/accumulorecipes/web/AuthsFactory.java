package org.calrissian.accumulorecipes.web;

import javax.servlet.http.HttpServletRequest;

import org.calrissian.accumulorecipes.commons.domain.Auths;

public interface AuthsFactory {

    Auths buildAuths(HttpServletRequest request);
}
