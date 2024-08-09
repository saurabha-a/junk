package com.unilog.prime.commons.service;

import com.unilog.prime.commons.model.Captcha;

public interface ICaptchaService {
	
	Captcha getCaptcha(int width,int height);

}
