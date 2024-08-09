package com.unilog.prime.commons.util;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Component;

import com.unilog.prime.commons.enumeration.PrimeResponseCode;
import com.unilog.prime.commons.exception.PrimeException;
import com.unilog.prime.commons.model.PasswordPolicy;

@Component
public class PasswordValidator {

	@Autowired
	private PasswordEncoder passwordEncoder;

	public boolean isValidPassword(String password, PasswordPolicy realmPasswordPolicy) {
		if (password == null || password == "")
			throw new PrimeException(HttpStatus.UNAUTHORIZED, "Invalid  Password or PolicyCode",
					PrimeResponseCode.ACCESS_DENIED.getResponseCode());
		else if (realmPasswordPolicy == null || realmPasswordPolicy.getPasswordPolicy() == null)
			return true;
		return validatePolicy(realmPasswordPolicy.getPasswordPolicy(), password, realmPasswordPolicy);
	}

	private boolean validatePolicy(int policyCode, String password, PasswordPolicy realmPasswordPolicy) {
		if ((policyCode & 1) != 0 && !this.checkRex("[a-z]", password))
			this.validationException("Password should contain atleast one Lowercase");
		if ((policyCode & 2) != 0 && !this.checkRex("[A-Z]", password))
			this.validationException("Password should contain atleast one Uppercase");
		if ((policyCode & 4) != 0 && !this.checkRex("[0-9]", password))
			this.validationException("Password should contain atleast one Number");
		if ((policyCode & 8) != 0 && !this.checkRex("[\\!^&()-_?=.*[@#$%]]", password))
			this.validationException("Password should contain atleast one Special Character");
		if (realmPasswordPolicy == null)
			return true;
		if (realmPasswordPolicy.getPasswordMinLength() != null && (policyCode & 16) != 0 && password.length() < realmPasswordPolicy.getPasswordMinLength())
			this.validationException(
					"Password should  be minimum " + realmPasswordPolicy.getPasswordMinLength() + " characters");
		if (realmPasswordPolicy.getPasswordMaxLength() != null && (policyCode & 32) != 0 && password.length() > realmPasswordPolicy.getPasswordMaxLength())
			this.validationException(
					"Password should not exceed " + realmPasswordPolicy.getPasswordMaxLength() + " characters");
		if ((policyCode & 64) != 0)
			this.checkPasswordHistory(password, realmPasswordPolicy.getPasswordHistory());
		if ((policyCode & 128) != 0 && password.indexOf(' ') != -1)
			this.validationException("Password should not contain Space");
		if ((policyCode & 256) != 0 && (password.indexOf(realmPasswordPolicy.getFirstName()) != -1
				|| password.indexOf(realmPasswordPolicy.getLastName()) != -1))
			this.validationException("Password should not contain FirstName or LastName of the User");
		return true;

	}

	private boolean checkRex(String regex, String text) {
		Matcher matcher = Pattern.compile(regex).matcher(text);
		return matcher.find();
	}

	private void checkPasswordHistory(String password, List<String> resultRecord) {
		if (resultRecord == null)
			return;
		boolean matched = resultRecord.stream().anyMatch(e -> passwordEncoder.matches(password, e));
		if (matched)
			this.validationException("Password should not match with any of the old passwords");
	}

	private void validationException(String message) {
		throw new PrimeException(HttpStatus.BAD_REQUEST, message, HttpStatus.BAD_REQUEST.ordinal());
	}

}
