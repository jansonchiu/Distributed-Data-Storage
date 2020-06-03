start_ip_last_digits=100
start_port=8100
initial_node_count=5
view_str=""
initial_shard_count=2

# Initialize view

declare -a view
for (( i=1; i<=$initial_node_count; i++ ))
do
    ip="10.10.0.`expr $start_ip_last_digits + $i`"
    socket="${ip}:8085"

    view[`expr $i - 1`]=$socket
done

for (( i=0; i<${#view[@]}; i++ ))
do
    view_str="$view_str${view[$i]}"
    
    # Append a comma, except last element.
    if [ $i -lt `expr ${#view[@]} - 1` ]
    then
        view_str="$view_str,"
    fi
done

# Main commands

# Run nodes.
if [ "$1" = "run" ]
then
    # Run a single node.
    if [ $(echo $2 | cut -c1-4) = "node" ]
    then
        node_index=$(echo $2 | cut -c5-5)
        node_port=`expr $start_port + $node_index`
        node_ip="10.10.0.`expr $start_ip_last_digits + $node_index`"
        node_socket="${node_ip}:8085"

        cmd="docker run -it --mount"
        cmd="$cmd \"type=bind,src=$(pwd)/,target=/app\""
        cmd="$cmd -p $node_port:8085"
        cmd="$cmd --net=assignment4-net --ip=$node_ip --name=$2"
        cmd="$cmd -e SOCKET_ADDRESS=\"$node_socket\""
        cmd="$cmd -e VIEW=\"$view_str\""
        if [ $node_index -le $initial_node_count ]
        then
            cmd="$cmd -e SHARD_COUNT=\"$initial_shard_count\""
        fi
        cmd="$cmd assignment4-img"
        echo $cmd
        eval $cmd

        #docker run -it --mount "type=bind,src=$(pwd)/,target=/app" -p "$node_port:8085" --net=assignment4-net --ip="$node_ip" --name="$2" -e SOCKET_ADDRESS="$node_socket" -e VIEW="$view_str" -e SHARD_COUNT="$initial_shard_count" assignment4-img
    fi
fi

# Clean all nodes.
if [ "$1" = "clean" ]
then
    for (( i=1; i<=10; i++ ))
    do
        node_name="node$i"

        docker container rm -f "$node_name"
    done
fi

