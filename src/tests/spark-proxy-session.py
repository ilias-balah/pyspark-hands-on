from pyspark.sql.session import SparkSession
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


def test_session_creation_and_properties():
    """
    Test that the proxy successfully creates a SparkSession instance
    with the correct application name.
    """
    ssp = SparkSessionProxy(app_name := "test-spark-session-proxy").start()

    # Check that a spark session is created and assigned to 'session'
    # attribute.
    assert ssp.session is not None, "No session created for the proxy."

    # Check that the created session object is an instance of SparkSession.
    # This confirms that the session was successfully initialized
    # through the proxy.
    assert isinstance(ssp.session, SparkSession), ("Got unexpected type '{}' "
                                                   "for the created session"
                                                   .format(ssp.session.__name__))

    # Confirm that the Spark session was initialized with the correct
    # app name. This validates the propagation of user-defined properties
    # into the Spark environment.
    assert ssp.session_app_name == app_name, ("The created session's app "
                                              "name was set incorrectly : "
                                              "expected '{}', but got '{}'."
                                              .format(app_name, ssp.session_app_name))

    # Clean up the session after the test to avoid resource leaks.
    ssp.stop()

