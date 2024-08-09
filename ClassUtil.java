package com.unilog.prime.commons.util;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.reflect.FieldUtils;
import org.springframework.http.HttpStatus;

import com.unilog.prime.commons.exception.PrimeException;

public class ClassUtil {
	
	private ClassUtil() {
		
	}

	protected static final Set<Class<?>> WRAPPER_TYPES = getWrapperTypes();

	public static final String GETTER_METHOD_PREFIX = "get";
	public static final String GETTER_METHOD_PREFIX_FOR_BOOLEAN = "is";

	public static List<Field> getAllFieldsOfClass0(Class<?> clazz) {
		List<Field> fields = new ArrayList<>();
		Class<?> lClazz = clazz;
		do {
			fields.addAll(Arrays.asList(lClazz.getDeclaredFields()));
			lClazz = lClazz.getSuperclass();
		} while(hasSuperClass(lClazz));
		
		return fields;
	}

	private static Set<Class<?>> getWrapperTypes() {
		Set<Class<?>> ret = new HashSet<>();
		ret.add(Boolean.class);
		ret.add(Character.class);
		ret.add(Byte.class);
		ret.add(Short.class);
		ret.add(Integer.class);
		ret.add(Long.class);
		ret.add(Float.class);
		ret.add(Double.class);
		ret.add(Void.class);
		return ret;
	}

	public static <T> T newInstance(Class<T> clazz) {
		if (clazz == null) {
			return null;
		}

		try {
			return clazz.getDeclaredConstructor().newInstance();
		} catch (Exception e) {
			throw new PrimeException(HttpStatus.INTERNAL_SERVER_ERROR,"Failed to instantiate class: " + clazz.getName(), e);
		}
	}

	@SuppressWarnings("unchecked")
	public static <T> Class<T> getWrapperTypeIfPrimitive(Class<T> type) {
		if (type == null) {
			return null;
		}

		if (type.isPrimitive()) {
			return ((PrimitiveInfo<T>) PRIMITIVES.get(type.getName())).wrapperType;
		}

		return type;
	}

	private static final Map<String, PrimitiveInfo<?>> PRIMITIVES = new HashMap<>(13);

	static {
		addPrimitive(boolean.class, "Z", Boolean.class, "booleanValue", false, Boolean.FALSE, Boolean.TRUE);
		addPrimitive(short.class, "S", Short.class, "shortValue", (short) 0, Short.MAX_VALUE, Short.MIN_VALUE);
		addPrimitive(int.class, "I", Integer.class, "intValue", 0, Integer.MAX_VALUE, Integer.MIN_VALUE);
		addPrimitive(long.class, "J", Long.class, "longValue", 0L, Long.MAX_VALUE, Long.MIN_VALUE);
		addPrimitive(float.class, "F", Float.class, "floatValue", 0F, Float.MAX_VALUE, Float.MIN_VALUE);
		addPrimitive(double.class, "D", Double.class, "doubleValue", 0D, Double.MAX_VALUE, Double.MIN_VALUE);
		addPrimitive(char.class, "C", Character.class, "charValue", '\0', Character.MAX_VALUE, Character.MIN_VALUE);
		addPrimitive(byte.class, "B", Byte.class, "byteValue", (byte) 0, Byte.MAX_VALUE, Byte.MIN_VALUE);
		addPrimitive(void.class, "V", Void.class, null, null, null, null);
	}

	private static <T> void addPrimitive(Class<T> type, String typeCode, Class<T> wrapperType, String unwrapMethod,
			T defaultValue, T maxValue, T minValue) {
		PrimitiveInfo<T> info = new PrimitiveInfo<>(type, typeCode, wrapperType, unwrapMethod, defaultValue, maxValue,
				minValue);

		PRIMITIVES.put(type.getName(), info);
		PRIMITIVES.put(wrapperType.getName(), info);
	}

	private static class PrimitiveInfo<T> {
		@SuppressWarnings("unused")
		final Class<T> type;
		@SuppressWarnings("unused")
		final String typeCode;
		final Class<T> wrapperType;
		@SuppressWarnings("unused")
		final String unwrapMethod;
		@SuppressWarnings("unused")
		final T defaultValue;
		@SuppressWarnings("unused")
		final T maxValue;
		@SuppressWarnings("unused")
		final T minValue;

		public PrimitiveInfo(Class<T> type, String typeCode, Class<T> wrapperType, String unwrapMethod, T defaultValue,
				T maxValue, T minValue) {
			this.type = type;
			this.typeCode = typeCode;
			this.wrapperType = wrapperType;
			this.unwrapMethod = unwrapMethod;
			this.defaultValue = defaultValue;
			this.maxValue = maxValue;
			this.minValue = minValue;
		}
	}

	public static boolean isWrapperType(Class<?> clazz) {
		return WRAPPER_TYPES.contains(clazz);
	}

	public static boolean isClassCollection(Class<?> clazz) {
		return Collection.class.isAssignableFrom(clazz);
	}

	public static boolean isClassMap(Class<?> clazz) {
		return Map.class.isAssignableFrom(clazz);
	}

	public static boolean isTypeOf(String myClass, Class<?> superClass) {
		boolean isSubclassOf = false;
		try {
			if (myClass != null) {
				Class<?> clazz = Class.forName(myClass);
				if (!clazz.equals(superClass)) {
					clazz = clazz.getSuperclass();
					if (clazz != null) {
						isSubclassOf = isTypeOf(clazz.getName(), superClass);
					} else {
						isSubclassOf = false;
					}
				} else {
					isSubclassOf = true;
				}
			}
		} catch (ClassNotFoundException e) {
			/* Ignore */
		}
		return isSubclassOf;
	}

	public static boolean hasSuperClass(Class<?> clazz) {
		return (clazz != null) && !clazz.equals(Object.class);
	}

	public static Method getGetterMethod(Class<?> clazz, Field field) {
		return ClassUtil.getMethod(clazz, ClassUtil.getGetterMethodName(field));
	}

	public static <A extends Annotation> A getAnnotation(Field field, Class<A> annotationType) {
		if (field == null || annotationType == null) {
			return null;
		}

		return field.getAnnotation(annotationType);
	}

	public static Method getMethod(Class<?> clazz, String methodName, Class<?>... parameterTypes) {
		if (clazz == null || methodName == null || methodName.length() == 0) {
			return null;
		}
		Class<?> lClazz = clazz;
		do {
			Method[] methods = lClazz.getDeclaredMethods();

			for (Method method : methods) {
				if (method.getName().equals(methodName) && Arrays.equals(method.getParameterTypes(), parameterTypes)) {
					return method;
				}
			}

			lClazz = lClazz.getSuperclass();
		} while(hasSuperClass(lClazz));
		
		return null;
	}

	public static String getGetterMethodName(Field field) {
		String prefix;
		String fieldName = field.getName();
		if (field.getType() != boolean.class || field.getType() != Boolean.class) {
			prefix = GETTER_METHOD_PREFIX;
		} else {
			prefix = GETTER_METHOD_PREFIX_FOR_BOOLEAN;
		}
		return prefix + fieldName.substring(0, 1).toUpperCase() + fieldName.substring(1);
	}

	public static Field[] getAnnotationFields(Class<?> clazz, Class<? extends Annotation> annotationClass) {
		return FieldUtils.getFieldsWithAnnotation(clazz, annotationClass);
	}

	public static List<Field> getAnnotationFieldsList(Class<?> clazz, Class<? extends Annotation> annotationClass) {
		return FieldUtils.getFieldsListWithAnnotation(clazz, annotationClass);
	}

	public static Map<String, Field> getFieldsMap(Class<?> clazz) {
		Map<String, Field> fields = new HashMap<>();

		for (Field field : FieldUtils.getAllFieldsList(clazz)) {
			if (field.getName().equals("serialVersionUID")) {
				continue;
			}
			if (!fields.containsKey(field.getName())) {
				fields.put(field.getName(), field);
			}
		}

		return fields;
	}
	

    public static List<String> getParameterNames(Method method) {
        Parameter[] parameters = method.getParameters();
        List<String> parameterNames = new ArrayList<>();

        for (Parameter parameter : parameters) {
            if(!parameter.isNamePresent()) {
                throw new IllegalArgumentException("Parameter names are not present!");
            }

            String parameterName = parameter.getName();
            parameterNames.add(parameterName);
        }

        return parameterNames;
    }

	/**
	 * @param entityType
	 * @return
	 * String
	 */
	public static String getResourceSimpleName(Class<?> entityType) {
		return entityType.getSimpleName();
	}
	

	@SuppressWarnings("unchecked")
	public static <T extends Number & Comparable<?>> T castToDestinationObject(String value,
			Class<? extends T> destinationClass) {
			
		if (destinationClass == short.class || destinationClass == Short.class) {
			return (T) Short.valueOf(value);
		} else if (destinationClass == int.class || destinationClass == Integer.class) {
			return (T) Integer.valueOf(value);
		} else if (destinationClass == long.class || destinationClass == Long.class) {
			return (T) Long.valueOf(value);
		} else if (destinationClass == float.class || destinationClass == Float.class) {
			return (T) Float.valueOf(value);
		} else if ((destinationClass == double.class || destinationClass == Double.class)) {
			return (T) Double.valueOf(value);
		} else if (destinationClass == BigDecimal.class) {
			return (T) new BigDecimal(value);
		}

		throw new ClassCastException("cannot convert values of type '" + value.getClass().getName() + "' into type '"
				+ destinationClass + "'");
	}

}