/*
 * Copyright (c) 2020, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.micro.integrator.ntask.coordination.task.store.connector;

import org.wso2.micro.integrator.ntask.coordination.task.CoordinatedTask;

/**
 * The class which contains all the data base queries for the task database.
 */
public class TaskQueryHelper {

    //task table Name
    public static final String TABLE_NAME = "COORDINATED_TASK_TABLE";
    public static final String TASK_DELETE_GUARD_TABLE = "TASK_DELETE_GUARD";
    public static final String TASK_DELETE_BARRIER_TABLE = "TASK_DELETE_BARRIER";
    public static final String TASK_DELETE_BARRIER_EXPECTED_TABLE = "TASK_DELETE_BARRIER_EXPECTED";
    public static final String TASK_DELETE_BARRIER_ACK_TABLE = "TASK_DELETE_BARRIER_ACK";

    //message processor table Name
    public static final String MP_TABLE_NAME = "MP_STATE_TABLE";

    //MP table columns
    public static final String MP_NAME = "MP_NAME";
    public static final String MP_STATE = "MP_STATE";

    // Task table columns
    public static final String TASK_NAME = "TASK_NAME";
    public static final String DESTINED_NODE_ID = "DESTINED_NODE_ID";
    public static final String TASK_STATE = "TASK_STATE";
    public static final String GUARD_UUID = "GUARD_UUID";
    public static final String OWNER_NODE_ID = "OWNER_NODE_ID";
    public static final String STATUS = "STATUS";
    public static final String DEADLINE_AT = "DEADLINE_AT";
    public static final String UPDATED_AT = "UPDATED_AT";
    public static final String NODE_ID = "NODE_ID";
    public static final String ACKED_AT = "ACKED_AT";

    public static final String BARRIER_STATUS_OPEN = "OPEN";
    public static final String BARRIER_STATUS_FINALIZING = "FINALIZING";
    public static final String TASK_DELETE_PENDING_STATE = "DELETE_PENDING";

    // Latest known running-task snapshot per (node, task), used by monitoring and duplicate detection.
    public static final String RUNNING_TASK_OBSERVATION_TABLE = "RUNNING_TASK_OBSERVATION";
    public static final String OBSERVED_AT = "OBSERVED_AT";

    // Keep the update unconditional. Writes for a node are already serialized by the observation writer,
    // and a timestamp guard would make future timestamps sticky after a clock step.
    static final String UPDATE_RUNNING_TASK_OBSERVATION =
            "UPDATE " + RUNNING_TASK_OBSERVATION_TABLE + " SET " + OBSERVED_AT + " = ? WHERE " + NODE_ID + " = ? AND "
                    + TASK_NAME + " = ?";

    static final String INSERT_RUNNING_TASK_OBSERVATION =
            "INSERT INTO " + RUNNING_TASK_OBSERVATION_TABLE + " (" + NODE_ID + ", " + TASK_NAME + ", " + OBSERVED_AT
                    + ") VALUES (?,?,?)";

    static final String DELETE_STALE_RUNNING_TASK_OBSERVATIONS_OF_NODE =
            "DELETE FROM " + RUNNING_TASK_OBSERVATION_TABLE + " WHERE " + NODE_ID + " = ? AND " + OBSERVED_AT + " < ?";

    static final String SELECT_FRESH_RUNNING_TASK_OBSERVATIONS =
            "SELECT " + NODE_ID + ", " + TASK_NAME + ", " + OBSERVED_AT + " FROM " + RUNNING_TASK_OBSERVATION_TABLE
                    + " WHERE " + OBSERVED_AT +  " >= ?";

    // History of duplicate-execution episodes opened and closed by the leader.
    public static final String TASK_DUPLICATION_EVENT_TABLE = "TASK_DUPLICATION_EVENT";
    public static final String NODES = "NODES";
    public static final String DESTINED_NODE = "DESTINED_NODE";
    public static final String DETECTED_AT = "DETECTED_AT";
    public static final String CLEARED_AT = "CLEARED_AT";
    public static final String SEVERITY = "SEVERITY";
    public static final String TASK_KIND = "TASK_KIND";
    public static final String SEVERITY_SUSTAINED = "SUSTAINED";
    public static final String SEVERITY_TRANSIENT = "TRANSIENT";

    static final String INSERT_DUPLICATION_EVENT =
            "INSERT INTO " + TASK_DUPLICATION_EVENT_TABLE + " (" + TASK_NAME + ", " + NODES + ", " + DESTINED_NODE
                    + ", " + DETECTED_AT + ", " + TASK_KIND + ") VALUES (?,?,?,?,?)";

    static final String SELECT_OPEN_DUPLICATION_EVENTS =
            "SELECT " + TASK_NAME + ", " + DETECTED_AT + ", " + SEVERITY + " FROM " + TASK_DUPLICATION_EVENT_TABLE
                    + " WHERE " + CLEARED_AT + " IS NULL";

    static final String MARK_DUPLICATION_EVENT_SUSTAINED =
            "UPDATE " + TASK_DUPLICATION_EVENT_TABLE + " SET " + SEVERITY + " = '" + SEVERITY_SUSTAINED + "' WHERE "
                    + TASK_NAME + " = ? AND " + CLEARED_AT + " IS NULL";

    static final String CLOSE_DUPLICATION_EVENT =
            "UPDATE " + TASK_DUPLICATION_EVENT_TABLE + " SET " + CLEARED_AT + " = ?, " + SEVERITY + " = CASE WHEN "
                    + SEVERITY + " IS NULL THEN '" + SEVERITY_TRANSIENT + "' ELSE " + SEVERITY + " END WHERE "
                    + TASK_NAME + " = ? AND " + CLEARED_AT + " IS NULL";

    private static final String TASK_STATE_CONST =
            "( CASE " + TASK_STATE + " WHEN '" + CoordinatedTask.States.RUNNING + "' THEN '"
                    + CoordinatedTask.States.NONE + "' WHEN '" + CoordinatedTask.States.DEACTIVATED + "'THEN '"
                    + CoordinatedTask.States.PAUSED + "' ELSE " + TASK_STATE + " END )";

    static final String ADD_TASK =
            "INSERT INTO " + TABLE_NAME + " ( " + TASK_NAME + ", " + DESTINED_NODE_ID + ", " + TASK_STATE + ") "
                    + "VALUES (?,NULL,?)";

    static final String UPDATE_ASSIGNMENT_AND_STATE =
            "UPDATE  " + TABLE_NAME + " SET  " + DESTINED_NODE_ID + " = ? , " + TASK_STATE + " = " + TASK_STATE_CONST
                    + " WHERE " + TASK_NAME + " = ?";

    static final String UPDATE_TASK_STATUS_TO_DEACTIVATED =
            "UPDATE  " + TABLE_NAME + "  SET " + TASK_STATE + " = '" + CoordinatedTask.States.DEACTIVATED + "' "
                    + "WHERE " + TASK_NAME + " =? AND " + TASK_STATE + " !='" + CoordinatedTask.States.PAUSED + "'";

    static final String ACTIVATE_TASK =
            "UPDATE  " + TABLE_NAME + "  SET " + TASK_STATE + " = '" + CoordinatedTask.States.ACTIVATED + "' WHERE "
                    + TASK_NAME + " =? AND " + TASK_STATE + " !='" + CoordinatedTask.States.RUNNING + "'";

    static final String UPDATE_TASK_STATE =
            "UPDATE  " + TABLE_NAME + "  SET " + TASK_STATE + " = ? WHERE " + TASK_NAME + " =? ";

    static final String UPDATE_MP_STATE =
            "UPDATE " + MP_TABLE_NAME + " SET " + MP_STATE + " = ? WHERE " + MP_NAME + " =?";

    static final String INSERT_MP_STATE =
            "INSERT INTO " + MP_TABLE_NAME + " ( " + MP_NAME + ", " + MP_STATE + ") VALUES (?, ?)";

    static final String UPDATE_TASK_STATE_FOR_DESTINED_NODE =
            "UPDATE  " + TABLE_NAME + "  SET " + TASK_STATE + " = ? WHERE " + TASK_NAME + " =? AND " + DESTINED_NODE_ID
                    + " =?";

    static final String RETRIEVE_ALL_TASKS = "SELECT  " + TASK_NAME  + "," + DESTINED_NODE_ID + " FROM " + TABLE_NAME;

    static final String RETRIEVE_UNASSIGNED_NOT_COMPLETED_TASKS =
            "SELECT " + TASK_NAME + " FROM " + TABLE_NAME + " WHERE  " + DESTINED_NODE_ID + " IS NULL AND " + TASK_STATE
                    + " !='" + CoordinatedTask.States.COMPLETED + "' AND " + TASK_STATE + " !='"
                    + TASK_DELETE_PENDING_STATE + "'";

    static final String RETRIEVE_TASKS_OF_NODE =
            "SELECT " + TASK_NAME + " FROM " + TABLE_NAME + "  WHERE " + DESTINED_NODE_ID + " =? AND " + TASK_STATE
                    + " =?";

    static final String RETRIEVE_TASK_STATE =
            "SELECT " + TASK_STATE + " FROM " + TABLE_NAME + "  WHERE " + TASK_NAME + " =?";

    static final String RETRIEVE_MP_STATE =
            "SELECT " + MP_STATE + " FROM " + MP_TABLE_NAME + "  WHERE " + MP_NAME + " =?";

    static final String REMOVE_ASSIGNMENT_AND_UPDATE_STATE =
            "UPDATE " + TABLE_NAME + " SET " + DESTINED_NODE_ID + " = NULL , " + TASK_STATE + " = " + TASK_STATE_CONST
                    + " WHERE " + TASK_NAME + " =?";

    static final String REMOVE_TASKS_OF_NODE = "DELETE FROM " + TABLE_NAME + "  WHERE " + DESTINED_NODE_ID + " =? AND "
            + TASK_STATE + " NOT IN ('" + CoordinatedTask.States.COMPLETED + "', '" + CoordinatedTask.States.ACTIVATED
            + "', '" + CoordinatedTask.States.DEACTIVATED + "')";

    static final String DELETE_TASK = "DELETE FROM " + TABLE_NAME + " WHERE " + TASK_NAME + " =?";

    static final String CLEAN_TASKS_OF_NODE =
            "UPDATE " + TABLE_NAME + " SET " + DESTINED_NODE_ID + " = NULL , " + TASK_STATE + " = " + TASK_STATE_CONST
                    + " WHERE " + DESTINED_NODE_ID + " = ? AND " + TASK_STATE + " !='"
                    + CoordinatedTask.States.COMPLETED + "'";

    static final String GET_ALL_ASSIGNED_INCOMPLETE_TASKS =
            "SELECT * FROM " + TABLE_NAME + " WHERE " + DESTINED_NODE_ID + " IS NOT NULL AND " + TASK_STATE + " != '"
                    + CoordinatedTask.States.COMPLETED + "'";

    static final String INSERT_TASK_DELETE_GUARD =
            "INSERT INTO " + TASK_DELETE_GUARD_TABLE + " (" + TASK_NAME + ", " + GUARD_UUID + ", " + UPDATED_AT
                    + ") VALUES (?,?,?)";

    static final String UPDATE_TASK_DELETE_GUARD =
            "UPDATE " + TASK_DELETE_GUARD_TABLE + " SET " + GUARD_UUID + " = ?, " + UPDATED_AT + " = ? WHERE "
                    + TASK_NAME + " = ?";

    static final String UPDATE_TASK_DELETE_GUARD_IF_MATCH =
            "UPDATE " + TASK_DELETE_GUARD_TABLE + " SET " + GUARD_UUID + " = ?, " + UPDATED_AT + " = ? WHERE "
                    + TASK_NAME + " = ? AND " + GUARD_UUID + " = ?";

    static final String UPDATE_TASK_DELETE_GUARD_TIMESTAMP_IF_MATCH =
            "UPDATE " + TASK_DELETE_GUARD_TABLE + " SET " + UPDATED_AT + " = ? WHERE " + TASK_NAME
                    + " = ? AND " + GUARD_UUID + " = ?";

    static final String SELECT_TASK_DELETE_GUARD =
            "SELECT " + GUARD_UUID + " FROM " + TASK_DELETE_GUARD_TABLE + " WHERE " + TASK_NAME + " = ?";

    static final String SELECT_MAX_TASK_DELETE_GUARD_UPDATED_AT =
            "SELECT MAX(" + UPDATED_AT + ") FROM " + TASK_DELETE_GUARD_TABLE;

    static final String INSERT_TASK_DELETE_BARRIER =
            "INSERT INTO " + TASK_DELETE_BARRIER_TABLE + " (" + TASK_NAME + ", " + GUARD_UUID + ", "
                    + OWNER_NODE_ID + ", " + STATUS + ", " + DEADLINE_AT + ", " + UPDATED_AT
                    + ") VALUES (?,?,?,?,?,?)";

    static final String INSERT_TASK_DELETE_BARRIER_EXPECTED =
            "INSERT INTO " + TASK_DELETE_BARRIER_EXPECTED_TABLE + " (" + TASK_NAME + ", " + GUARD_UUID + ", "
                    + NODE_ID + ") VALUES (?,?,?)";

    static final String INSERT_TASK_DELETE_BARRIER_ACK =
            "INSERT INTO " + TASK_DELETE_BARRIER_ACK_TABLE + " (" + TASK_NAME + ", " + GUARD_UUID + ", " + NODE_ID
                    + ", " + ACKED_AT + ") VALUES (?,?,?,?)";

    static final String UPDATE_TASK_DELETE_BARRIER_ACK =
            "UPDATE " + TASK_DELETE_BARRIER_ACK_TABLE + " SET " + ACKED_AT + " = ? WHERE " + TASK_NAME + " = ? AND "
                    + GUARD_UUID + " = ? AND " + NODE_ID + " = ?";

    static final String UPDATE_TASK_DELETE_BARRIER_STATUS =
            "UPDATE " + TASK_DELETE_BARRIER_TABLE + " SET " + STATUS + " = ?, " + UPDATED_AT + " = ? WHERE "
                    + TASK_NAME + " = ? AND " + GUARD_UUID + " = ? AND " + STATUS + " = ?";

    static final String UPDATE_TASK_DELETE_BARRIER_TIMESTAMP =
            "UPDATE " + TASK_DELETE_BARRIER_TABLE + " SET " + UPDATED_AT + " = ? WHERE " + TASK_NAME + " = ? AND "
                    + GUARD_UUID + " = ?";

    static final String UPDATE_TASK_STATUS_TO_DELETE_PENDING =
            "UPDATE " + TABLE_NAME + " SET " + DESTINED_NODE_ID + " = NULL , " + TASK_STATE + " = '"
                    + TASK_DELETE_PENDING_STATE + "' WHERE " + TASK_NAME + " = ? AND " + TASK_STATE + " != '"
                    + CoordinatedTask.States.COMPLETED + "'";

    static final String DELETE_TASK_IF_STATE_MATCH =
            "DELETE FROM " + TABLE_NAME + " WHERE " + TASK_NAME + " = ? AND " + TASK_STATE + " = ?";

    static final String DELETE_TASK_IF_STATE_NOT_MATCH =
            "DELETE FROM " + TABLE_NAME + " WHERE " + TASK_NAME + " = ? AND " + TASK_STATE + " <> ?";

    static final String SELECT_OPEN_TASK_DELETE_BARRIER_BY_TASK_AND_GUARD =
            "SELECT " + TASK_NAME + ", " + GUARD_UUID + ", " + OWNER_NODE_ID + ", " + STATUS + ", " + DEADLINE_AT
                    + ", " + UPDATED_AT + " FROM " + TASK_DELETE_BARRIER_TABLE + " WHERE " + TASK_NAME + " = ? AND "
                    + GUARD_UUID + " = ? AND " + STATUS + " = ?";

    static final String SELECT_OPEN_TASK_DELETE_BARRIERS =
            "SELECT " + TASK_NAME + ", " + GUARD_UUID + ", " + OWNER_NODE_ID + ", " + STATUS + ", " + DEADLINE_AT
                    + ", " + UPDATED_AT + " FROM " + TASK_DELETE_BARRIER_TABLE + " WHERE " + STATUS + " = ?";

    static final String SELECT_TASK_DELETE_BARRIER =
            "SELECT " + TASK_NAME + ", " + GUARD_UUID + ", " + OWNER_NODE_ID + ", " + STATUS + ", " + DEADLINE_AT
                    + ", " + UPDATED_AT + " FROM " + TASK_DELETE_BARRIER_TABLE + " WHERE " + TASK_NAME + " = ? AND "
                    + GUARD_UUID + " = ?";

    static final String SELECT_TASK_DELETE_BARRIER_EXPECTED_NODES =
            "SELECT " + NODE_ID + " FROM " + TASK_DELETE_BARRIER_EXPECTED_TABLE + " WHERE " + TASK_NAME + " = ? AND "
                    + GUARD_UUID + " = ?";

    static final String SELECT_TASK_DELETE_BARRIER_ACK_NODES =
            "SELECT " + NODE_ID + " FROM " + TASK_DELETE_BARRIER_ACK_TABLE + " WHERE " + TASK_NAME + " = ? AND "
                    + GUARD_UUID + " = ?";

    static final String DELETE_TASK_DELETE_BARRIER_ACKS =
            "DELETE FROM " + TASK_DELETE_BARRIER_ACK_TABLE + " WHERE " + TASK_NAME + " = ? AND " + GUARD_UUID + " = ?";

    static final String DELETE_TASK_DELETE_BARRIER_EXPECTED =
            "DELETE FROM " + TASK_DELETE_BARRIER_EXPECTED_TABLE + " WHERE " + TASK_NAME + " = ? AND " + GUARD_UUID
                    + " = ?";

    static final String DELETE_TASK_DELETE_BARRIER =
            "DELETE FROM " + TASK_DELETE_BARRIER_TABLE + " WHERE " + TASK_NAME + " = ? AND " + GUARD_UUID + " = ?";

    private TaskQueryHelper() throws IllegalAccessException {
        throw new IllegalAccessException("This class not to be initialized.");
    }

}
