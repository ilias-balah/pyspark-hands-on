from src.internal.spark.proxies import SparkSessionProxy



def test_configs_initialization():
    """
    Test that the proxy correctly initializes its configuration as a dictionary,
    and that the application name is properly set.
    """
    ssp = SparkSessionProxy(app_name := "test-spark-session-proxy-configs")

    # Check that the 'configs' attribute exists on the proxy instance.
    # This ensures that internal setup includes config management.
    assert hasattr(ssp, 'configs'), "'configs' attribute was not found."
    
    # Check that 'configs' is a dictionary to guarantee compatibility
    # with Spark configuration methods.
    assert isinstance(ssp.configs, dict), ("Got unexpected type '{}' "
                                           "for 'configs' attribute."
                                           .format(ssp.configs.__name__))
    
    # Verify that the application name was properly injected into the
    # Spark config. This is essential because it confirms that user-specified
    # overrides take effect.
    assert ssp.configs_app_name == app_name, ("'spark.app.name' is not configured "
                                              "properly : expected '{}', but got '{}'."
                                              .format(ssp.configs_app_name, app_name))