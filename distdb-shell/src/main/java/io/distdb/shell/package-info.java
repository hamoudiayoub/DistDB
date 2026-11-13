/**
 * {@code distdb sql}: the interactive shell. {@link io.distdb.shell.PgClient} speaks PostgreSQL's
 * wire protocol, {@link io.distdb.shell.Render} lays results out, and {@link io.distdb.shell.Shell}
 * runs the loop: line editing (JLine), meta commands, and script mode.
 */
package io.distdb.shell;
