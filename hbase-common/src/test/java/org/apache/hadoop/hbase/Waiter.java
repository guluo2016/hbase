/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase;

import static org.junit.Assert.fail;

import java.text.MessageFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A class that provides a standard waitFor pattern See details at
 * https://issues.apache.org/jira/browse/HBASE-7384
 */
@InterfaceAudience.Private
public final class Waiter {
  private static final Logger LOG = LoggerFactory.getLogger(Waiter.class);

  /**
   * System property name whose value is a scale factor to increase time out values dynamically used
   * in {@link #sleep(Configuration, long)}, {@link #waitFor(Configuration, long, Predicate)},
   * {@link #waitFor(Configuration, long, long, Predicate)}, and
   * {@link #waitFor(Configuration, long, long, boolean, Predicate)} method
   * <p/>
   * The actual time out value will equal to hbase.test.wait.for.ratio * passed-in timeout
   */
  public static final String HBASE_TEST_WAIT_FOR_RATIO = "hbase.test.wait.for.ratio";

  private static float HBASE_WAIT_FOR_RATIO_DEFAULT = 1;

  private static float waitForRatio = -1;

  public static final long DEFAULT_INTERVAL = 100;

  private Waiter() {
  }

  /**
   * Returns the 'wait for ratio' used in the {@link #sleep(Configuration, long)},
   * {@link #waitFor(Configuration, long, Predicate)},
   * {@link #waitFor(Configuration, long, long, Predicate)} and
   * {@link #waitFor(Configuration, long, long, boolean, Predicate)} methods of the class
   * <p/>
   * This is useful to dynamically adjust max time out values when same test cases run in different
   * test machine settings without recompiling & re-deploying code.
   * <p/>
   * The value is obtained from the Java System property or configuration setting
   * <code>hbase.test.wait.for.ratio</code> which defaults to <code>1</code>.
   * @param conf the configuration
   * @return the 'wait for ratio' for the current test run.
   */
  public static float getWaitForRatio(Configuration conf) {
    if (waitForRatio < 0) {
      // System property takes precedence over configuration setting
      if (System.getProperty(HBASE_TEST_WAIT_FOR_RATIO) != null) {
        waitForRatio = Float.parseFloat(System.getProperty(HBASE_TEST_WAIT_FOR_RATIO));
      } else {
        waitForRatio = conf.getFloat(HBASE_TEST_WAIT_FOR_RATIO, HBASE_WAIT_FOR_RATIO_DEFAULT);
      }
    }
    return waitForRatio;
  }

  /**
   * A predicate 'closure' used by the {@link Waiter#waitFor(Configuration, long, Predicate)} and
   * {@link Waiter#waitFor(Configuration, long, Predicate)} and
   * {@link Waiter#waitFor(Configuration, long, long, boolean, Predicate)} methods.
   */
  @InterfaceAudience.Private
  public interface Predicate<E extends Exception> {
    /**
     * Perform a predicate evaluation.
     * @return the boolean result of the evaluation.
     * @throws E thrown if the predicate evaluation could not evaluate.
     */
    boolean evaluate() throws E;
  }

  /**
   * A mixin interface, can be used with {@link Waiter} to explain failed state.
   */
  @InterfaceAudience.Private
  public interface ExplainingPredicate<E extends Exception> extends Predicate<E> {
    /**
     * Perform a predicate evaluation.
     * @return explanation of failed state
     */
    String explainFailure() throws E;
  }

  /**
   * Makes the current thread sleep for the duration equal to the specified time in milliseconds
   * multiplied by the {@link #getWaitForRatio(Configuration)}.
   * @param conf the configuration
   * @param time the number of milliseconds to sleep.
   */
  public static void sleep(Configuration conf, long time) {
    try {
      Thread.sleep((long) (getWaitForRatio(conf) * time));
    } catch (InterruptedException ex) {
      LOG.warn(MessageFormat.format("Sleep interrupted, {0}", ex.toString()));
    }
  }

  /**
   * Waits up to the duration equal to the specified timeout multiplied by the
   * {@link #getWaitForRatio(Configuration)} for the given {@link Predicate} to become
   * <code>true</code>, failing the test if the timeout is reached and the Predicate is still
   * <code>false</code>.
   * <p/>
   * @param conf      the configuration
   * @param timeout   the timeout in milliseconds to wait for the predicate.
   * @param predicate the predicate to evaluate.
   * @return the effective wait, in milli-seconds until the predicate becomes <code>true</code> or
   *         wait is interrupted otherwise <code>-1</code> when times out
   */
  public static <E extends Exception> long waitFor(Configuration conf, long timeout,
    Predicate<E> predicate) {
    return waitFor(conf, timeout, DEFAULT_INTERVAL, true, predicate);
  }

  /**
   * Waits up to the duration equal to the specified timeout multiplied by the
   * {@link #getWaitForRatio(Configuration)} for the given {@link Predicate} to become
   * <code>true</code>, failing the test if the timeout is reached and the Predicate is still
   * <code>false</code>.
   * <p/>
   * @param conf      the configuration
   * @param timeout   the max timeout in milliseconds to wait for the predicate.
   * @param interval  the interval in milliseconds to evaluate predicate.
   * @param predicate the predicate to evaluate.
   * @return the effective wait, in milli-seconds until the predicate becomes <code>true</code> or
   *         wait is interrupted otherwise <code>-1</code> when times out
   */
  public static <E extends Exception> long waitFor(Configuration conf, long timeout, long interval,
    Predicate<E> predicate) {
    return waitFor(conf, timeout, interval, true, predicate);
  }

  /**
   * Waits up to the duration equal to the specified timeout multiplied by the
   * {@link #getWaitForRatio(Configuration)} for the given {@link Predicate} to become
   * <code>true</code>, failing the test if the timeout is reached, the Predicate is still
   * <code>false</code> and failIfTimeout is set as <code>true</code>.
   * <p/>
   * @param conf          the configuration
   * @param timeout       the timeout in milliseconds to wait for the predicate.
   * @param interval      the interval in milliseconds to evaluate predicate.
   * @param failIfTimeout indicates if should fail current test case when times out.
   * @param predicate     the predicate to evaluate.
   * @return the effective wait, in milli-seconds until the predicate becomes <code>true</code> or
   *         wait is interrupted otherwise <code>-1</code> when times out
   */
  public static <E extends Exception> long waitFor(Configuration conf, long timeout, long interval,
    boolean failIfTimeout, Predicate<E> predicate) {
    return waitFor(conf, timeout, 0, interval, failIfTimeout, predicate);
  }

  /**
   * Waits up to the duration equal to the specified timeout multiplied by the
   * {@link #getWaitForRatio(Configuration)} for the given {@link Predicate} to become
   * <code>true</code>. This method first waits for the initial delay, then checks the
   * predicate, failing the test if the timeout is reached, the Predicate is still
   * <code>false</code> and failIfTimeout is set as <code>true</code>.
   * @param conf          the configuration
   * @param timeout       the timeout in milliseconds to wait for the predicate.
   * @param initialDelay  the initial delay in milliseconds before starting task.
   * @param interval      the interval in milliseconds to evaluate predicate.
   * @param failIfTimeout indicates if should fail current test case when times out.
   * @param predicate     the predicate to evaluate.
   * @return the effective wait, in milli-seconds until the predicate becomes <code>true</code> or
   *         wait is interrupted otherwise <code>-1</code> when times out.
   */
  public static <E extends Exception> long waitFor(Configuration conf, long timeout,
    long initialDelay, long interval, boolean failIfTimeout, Predicate<E> predicate) {
    long started = EnvironmentEdgeManager.currentTime();
    long adjustedTimeout = (long) (getWaitForRatio(conf) * timeout);
    long adjustedInitialDelay = (long) (getWaitForRatio(conf) * initialDelay);
    long mustEnd = started + adjustedTimeout;
    long remainderWait;
    long sleepInterval;
    boolean eval;
    boolean interrupted = false;

    try {
      if (initialDelay > 0) {
        LOG.info("Waiting for initial delay of {} ms before starting task (wait.for.ratio={})",
          adjustedInitialDelay, getWaitForRatio(conf));
        try {
          Thread.sleep(adjustedInitialDelay);
        } catch (InterruptedException e) {
          LOG.warn("Initial delay interrupted after {} ms",
            EnvironmentEdgeManager.currentTime() - started);
          eval = predicate.evaluate();
          return eval ? (EnvironmentEdgeManager.currentTime() - started) : -1;
        }
      }

      LOG.info("Waiting up to {} ms (wait.for.ratio={}) after initial delay of {} ms",
        adjustedTimeout, getWaitForRatio(conf), adjustedInitialDelay);
      while (
        !(eval = predicate.evaluate())
          && (remainderWait = mustEnd - EnvironmentEdgeManager.currentTime()) > 0
      ) {
        try {
          // handle tail case when remainder wait is less than one interval
          sleepInterval = Math.min(remainderWait, interval);
          Thread.sleep(sleepInterval);
        } catch (InterruptedException e) {
          eval = predicate.evaluate();
          interrupted = true;
          break;
        }
      }

      if (!eval) {
        long totalElapsed = EnvironmentEdgeManager.currentTime() - started;
        if (interrupted) {
          LOG.warn("Waiting interrupted after {} ms", totalElapsed);
        } else if (failIfTimeout) {
          String msg = getExplanation(predicate);
          fail(MessageFormat.format(
            "Waiting timed out after {0} ms (initial delay: {1} ms, polling timeout: {2} ms)",
            totalElapsed, adjustedInitialDelay, adjustedTimeout) + msg);
        } else {
          LOG.warn(
            "Waiting timed out after {} ms (initial delay: {} ms, polling timeout: {} ms), {}",
            totalElapsed, adjustedInitialDelay, adjustedTimeout, getExplanation(predicate));
        }
      }
      return (eval || interrupted) ? (EnvironmentEdgeManager.currentTime() - started) : -1;
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  public static String getExplanation(Predicate<?> explain) {
    if (explain instanceof ExplainingPredicate) {
      try {
        return " " + ((ExplainingPredicate<?>) explain).explainFailure();
      } catch (Exception e) {
        LOG.error("Failed to get explanation, ", e);
        return e.getMessage();
      }
    } else {
      return "";
    }
  }
}
