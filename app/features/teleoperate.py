import logging
import time
import threading
from concurrent.futures import ThreadPoolExecutor

# From lerobot
from lerobot.teleoperators.so101_leader import SO101LeaderConfig, SO101Leader
from lerobot.robots.so101_follower import SO101FollowerConfig, SO101Follower

# From app
from app.config import setup_calibration_files
from app.types.so_arm import TeleoperateRequest

logger = logging.getLogger(__name__)


class TeleoperationManager:
    def __init__(self):
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._executor = ThreadPoolExecutor(max_workers=1)
        self._future = None
        self._robot = None
        self._leader = None
        self._active = False

    def start(self, request: TeleoperateRequest):
        with self._lock:
            if self._active:
                return {
                    "success": False,
                    "message": "Teleoperation is already active",
                }
            self._stop.clear()

            # Prepare calibration and device configs
            try:
                logger.info(
                    f"Starting teleoperation with leader port: {request.leader_port}, follower port: {request.follower_port}"
                )

                leader_config_name, follower_config_name = setup_calibration_files(
                    request.leader_config, request.follower_config
                )

                follower_config = SO101FollowerConfig(
                    port=request.follower_port,
                    id=follower_config_name,
                )

                leader_config = SO101LeaderConfig(
                    port=request.leader_port,
                    id=leader_config_name,
                )
            except Exception as e:
                logger.error(f"Failed to prepare teleoperation configs: {e}")
                return {
                    "success": False,
                    "message": f"Failed to start teleoperation: {str(e)}",
                }

            def worker():
                try:
                    self._active = True

                    logger.info("Initializing leader and follower device...")
                    follower = SO101Follower(follower_config)
                    leader = SO101Leader(leader_config)

                    self._robot = follower
                    self._leader = leader

                    follower.bus.connect()
                    leader.bus.connect()

                    # Write calibration to motors' memory
                    follower.bus.write_calibration(follower.calibration)
                    leader.bus.write_calibration(leader.calibration)

                    # Connect cameras and configure motors
                    # logger.info("Connecting cameras and configuring motors...")
                    # for cam in follower.cameras.values():
                    #     cam.connect()
                    follower.configure()
                    leader.configure()

                    logger.info("Starting teleoperation loop...")
                    try:
                        while not self._stop.is_set():
                            action = leader.get_action()
                            follower.send_action(action)
                            time.sleep(0.001)
                    finally:
                        follower.disconnect()
                        leader.disconnect()
                        logger.info("Teleoperation stopped")

                except Exception as e:
                    logger.error(f"Error during teleoperation: {e}")
                finally:
                    self._active = False
                    self._robot = None
                    self._leader = None

            self._future = self._executor.submit(worker)
            return {"success": True, "message": "Teleoperation started"}

    def stop(self):
        with self._lock:
            if not self._active:
                return {"success": False, "message": "Teleoperation is not active"}
            self._stop.set()
            if self._future:
                try:
                    self._future.cancel()
                except Exception as e:
                    logger.error(f"Error stopping teleoperation: {e}")
                    pass
            return {
                "success": True,
                "message": "Teleoperation stopped",
            }

    def status(self):
        return {"teleoperation_active": self._active}


# def get_joint_positions_from_robot(robot) -> Dict[str, float]:
#     """
#     Extract current joint positions from the robot and convert to URDF joint format.

#     Args:
#         robot: The robot instance (SO101Follower)

#     Returns:
#         Dictionary mapping URDF joint names to radian values
#     """
#     try:
#         # Get the current observation from the robot
#         observation = robot.get_observation()

#         # Map robot motor names to URDF joint names
#         # Based on the motor configuration in SO101Follower and URDF joint names
#         motor_to_urdf_mapping = {
#             "shoulder_pan": "Rotation",  # Base rotation
#             "shoulder_lift": "Pitch",  # Shoulder pitch
#             "elbow_flex": "Elbow",  # Elbow flexion
#             "wrist_flex": "Wrist_Pitch",  # Wrist pitch
#             "wrist_roll": "Wrist_Roll",  # Wrist roll
#             "gripper": "Jaw",  # Gripper/jaw
#         }

#         joint_positions = {}

#         # Extract joint positions and convert degrees to radians
#         for motor_name, urdf_joint_name in motor_to_urdf_mapping.items():
#             motor_key = f"{motor_name}.pos"
#             if motor_key in observation:
#                 # Convert degrees to radians for the URDF viewer
#                 angle_degrees = observation[motor_key]
#                 angle_radians = angle_degrees * (3.14159 / 180.0)
#                 joint_positions[urdf_joint_name] = angle_radians
#             else:
#                 logger.warning(f"Motor {motor_key} not found in observation")
#                 joint_positions[urdf_joint_name] = 0.0

#         return joint_positions

#     except Exception as e:
#         logger.error(f"Error getting joint positions: {e}")
#         return {
#             "Rotation": 0.0,
#             "Pitch": 0.0,
#             "Elbow": 0.0,
#             "Wrist_Pitch": 0.0,
#             "Wrist_Roll": 0.0,
#             "Jaw": 0.0,
#         }


# def handle_start_teleoperation(
#     request: TeleoperateRequest, websocket_manager=None
# ) -> Dict[str, Any]:
#     """Handle start teleoperation request"""
#     global teleoperation_active, teleoperation_thread, current_robot, current_teleop

#     if teleoperation_active:
#         return {"success": False, "message": "Teleoperation is already active"}

#     try:
#         logger.info(
#             f"Starting teleoperation with leader port: {request.leader_port}, follower port: {request.follower_port}"
#         )

#         # Setup calibration files
#         leader_config_name, follower_config_name = setup_calibration_files(
#             request.leader_config, request.follower_config
#         )

#         # Create robot and teleop configs
#         robot_config = SO101FollowerConfig(
#             port=request.follower_port,
#             id=follower_config_name,
#         )

#         teleop_config = SO101LeaderConfig(
#             port=request.leader_port,
#             id=leader_config_name,
#         )

#         # Start teleoperation in a separate thread
#         def teleoperation_worker():
#             global teleoperation_active, current_robot, current_teleop
#             teleoperation_active = True

#             try:
#                 logger.info("Initializing robot and teleop device...")
#                 robot = SO101Follower(robot_config)
#                 teleop_device = SO101Leader(teleop_config)

#                 current_robot = robot
#                 current_teleop = teleop_device

#                 logger.info("Connecting to devices...")
#                 robot.bus.connect()
#                 teleop_device.bus.connect()

#                 # Write calibration to motors' memory
#                 logger.info("Writing calibration to motors...")
#                 robot.bus.write_calibration(robot.calibration)
#                 teleop_device.bus.write_calibration(teleop_device.calibration)

#                 # Connect cameras and configure motors
#                 logger.info("Connecting cameras and configuring motors...")
#                 for cam in robot.cameras.values():
#                     cam.connect()
#                 robot.configure()
#                 teleop_device.configure()
#                 logger.info("Successfully connected to both devices")

#                 logger.info("Starting teleoperation loop...")
#                 logger.info("Press 'q' to quit teleoperation")

#                 # Set up keyboard for non-blocking input
#                 old_settings = setup_keyboard()

#                 try:
#                     want_to_disconnect = False
#                     last_broadcast_time = 0
#                     broadcast_interval = 0.05  # Broadcast every 50ms (20 FPS)

#                     while not want_to_disconnect and teleoperation_active:
#                         # Check teleoperation_active flag first (for web stop requests)
#                         if not teleoperation_active:
#                             logger.info("Teleoperation stopped via web interface")
#                             break

#                         action = teleop_device.get_action()
#                         robot.send_action(action)

#                         # Broadcast joint positions to connected WebSocket clients
#                         current_time = time.time()
#                         if current_time - last_broadcast_time >= broadcast_interval:
#                             try:
#                                 joint_positions = get_joint_positions_from_robot(robot)
#                                 joint_data = {
#                                     "type": "joint_update",
#                                     "joints": joint_positions,
#                                     "timestamp": current_time,
#                                 }

#                                 # Use websocket manager to broadcast the data
#                                 if (
#                                     websocket_manager
#                                     and websocket_manager.active_connections
#                                 ):
#                                     websocket_manager.broadcast_joint_data_sync(
#                                         joint_data
#                                     )

#                                 last_broadcast_time = current_time
#                             except Exception as e:
#                                 logger.error(f"Error broadcasting joint data: {e}")

#                         # Check for keyboard input
#                         if check_quit_key():
#                             want_to_disconnect = True
#                             logger.info("Quit key pressed, stopping teleoperation...")

#                         # Small delay to prevent excessive CPU usage and allow for responsive stopping
#                         time.sleep(0.001)  # 1ms delay
#                 finally:
#                     # Always restore keyboard settings
#                     restore_keyboard(old_settings)
#                     robot.disconnect()
#                     teleop_device.disconnect()
#                     logger.info("Teleoperation stopped")

#                 return {
#                     "success": True,
#                     "message": "Teleoperation completed successfully",
#                 }

#             except Exception as e:
#                 logger.error(f"Error during teleoperation: {e}")
#                 return {"success": False, "error": str(e)}
#             finally:
#                 teleoperation_active = False
#                 current_robot = None
#                 current_teleop = None

#         teleoperation_thread = ThreadPoolExecutor(max_workers=1)
#         future = teleoperation_thread.submit(teleoperation_worker)

#         return {
#             "success": True,
#             "message": "Teleoperation started successfully",
#             "leader_port": request.leader_port,
#             "follower_port": request.follower_port,
#         }

#     except Exception as e:
#         teleoperation_active = False
#         logger.error(f"Failed to start teleoperation: {e}")
#         return {"success": False, "message": f"Failed to start teleoperation: {str(e)}"}


# def handle_get_joint_positions() -> Dict[str, Any]:
#     """Handle get current robot joint positions request"""
#     global current_robot

#     if not teleoperation_active or current_robot is None:
#         return {"success": False, "message": "No active teleoperation session"}

#     try:
#         joint_positions = get_joint_positions_from_robot(current_robot)
#         return {
#             "success": True,
#             "joint_positions": joint_positions,
#             "timestamp": time.time(),
#         }
#     except Exception as e:
#         logger.error(f"Error getting joint positions: {e}")
#         return {"success": False, "message": f"Failed to get joint positions: {str(e)}"}
