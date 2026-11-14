using System.Reflection;
using System.Runtime.Loader;

if (args.Length != 2) throw new ArgumentException("Usage: GroundYawChecks <Game2 directory> <Y51_SE2Online.dll>");
var gameDirectory = Path.GetFullPath(args[0]);
var pluginPath = Path.GetFullPath(args[1]);
AssemblyLoadContext.Default.Resolving += (_, name) =>
{
    foreach (var directory in new[] { Path.GetDirectoryName(pluginPath)!, gameDirectory })
    {
        var path = Path.Combine(directory, name.Name + ".dll");
        if (File.Exists(path)) return AssemblyLoadContext.Default.LoadFromAssemblyPath(path);
    }
    return null;
};
const BindingFlags all = BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance | BindingFlags.Static;
var plugin = AssemblyLoadContext.Default.LoadFromAssemblyPath(pluginPath);
var motion = plugin.GetType("Y51_SE2Online.KeenNetcode.OwnMotion", true)!;
var ground = motion.GetMethod("GroundYawPostfix", all)!;
var collect = motion.GetMethod("CollectCharacterPrefix", all)!;
var tick = motion.GetMethod("ClientTick", all)!;
var reset = motion.GetMethod("ResetClient", all)!;
var own = Activator.CreateInstance(ground.GetParameters()[0].ParameterType)!;
var controlType = ground.GetParameters()[1].ParameterType.GetElementType()!;
var transformType = collect.GetParameters()[0].ParameterType.GetElementType()!;
var softType = collect.GetParameters()[1].ParameterType.GetElementType()!;
var changeType = collect.GetParameters()[2].ParameterType.GetElementType()!;
var positionField = transformType.GetField("Position")!;
var orientationField = transformType.GetField("Orientation")!;
var yawField = motion.GetField("_groundYaw", all)!;
var ownAtField = motion.GetField("_ownAt", all)!;
var drivingField = motion.GetField("_driving", all)!;
motion.GetField("_own", all)!.SetValue(null, own);
motion.GetField("_shiftPending", all)!.SetValue(null, false);
motion.GetField("_softShiftPending", all)!.SetValue(null, false);
ownAtField.SetValue(null, Activator.CreateInstance(positionField.FieldType));
yawField.SetValue(null, 0f);
drivingField.SetValue(null, true);

object Identity() => transformType.GetField("Identity", all)!.GetValue(null)!;
object Input(float yaw)
{
    var control = Activator.CreateInstance(controlType)!;
    var rotation = controlType.GetField("Rotation")!;
    var value = Activator.CreateInstance(rotation.FieldType)!;
    value.GetType().GetField("Y")!.SetValue(value, yaw);
    rotation.SetValue(control, value);
    return control;
}
object Collect(object transform)
{
    var soft = Activator.CreateInstance(softType)!;
    softType.GetField("Value")!.SetValue(soft, transform);
    object[] arguments = [transform, soft, Activator.CreateInstance(changeType)!];
    collect.Invoke(null, arguments);
    return arguments[0];
}
void Check(bool condition, string message)
{
    if (!condition) throw new Exception(message);
}

ground.Invoke(null, [own, Input(.4f)]);
var identity = Identity();
var turned = Collect(identity);
Check((float)yawField.GetValue(null)! == 0f, "A successful collect must consume the cached turn.");
Check(!Equals(orientationField.GetValue(turned), orientationField.GetValue(identity)), "Normal grounded yaw must rotate the transform.");
var again = Collect(turned);
Check(Equals(orientationField.GetValue(again), orientationField.GetValue(turned)), "A consumed turn must not repeat.");

ground.Invoke(null, [own, Input(.4f)]);
var moved = Identity();
var position = Activator.CreateInstance(positionField.FieldType)!;
position.GetType().GetField("X")!.SetValue(position, 1d);
positionField.SetValue(moved, position);
var missed = Collect(moved);
Check((float)yawField.GetValue(null)! == .4f, "The position guard must leave the cached turn pending until the tick ends.");
Check(Equals(orientationField.GetValue(missed), orientationField.GetValue(identity)), "The guard-miss tick must not rotate.");
// ClientTick follows Session.Update, after physics; its no-client early return must also expire that tick's yaw.
tick.Invoke(null, [null]);
Check((float)yawField.GetValue(null)! == 0f, "The post-physics client tick must expire an unconsumed turn before early returns.");
ownAtField.SetValue(null, position);
drivingField.SetValue(null, true);
ground.Invoke(null, [own, Input(0f)]);
var afterTick = Collect(missed);
Check(Equals(orientationField.GetValue(afterTick), orientationField.GetValue(missed)), "Zero input after a guard miss must not replay the previous tick's turn.");

ground.Invoke(null, [own, Input(.4f)]);
Check((float)yawField.GetValue(null)! == .4f, "Reset check must begin with a pending turn.");
reset.Invoke(null, null);
Check((float)yawField.GetValue(null)! == 0f, "Leaving a session must clear cached yaw.");
var afterReset = Collect(missed);
Check(Equals(orientationField.GetValue(afterReset), orientationField.GetValue(missed)), "A disposed session's turn must not reach another collect.");
Console.WriteLine("PASS actual own-motion hooks: normal yaw once, missed collect expired at the post-physics tick, zero-input follow-up unchanged, session reset clears yaw. No world started.");
