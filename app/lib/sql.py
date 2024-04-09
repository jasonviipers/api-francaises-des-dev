from contextlib import contextmanager
from collections import namedtuple
from typing import Optional, List, Dict, Any

from fastapi import UploadFile

from app.models import *

from app import settings
from datetime import datetime, timedelta
import mysql.connector
from mysql.connector.pooling import MySQLConnectionPool

MemberHasCategoryOut = namedtuple("MemberHasCategoryOut", ["id_member", "name", "id_category"])

pool = MySQLConnectionPool(
    host=settings.HOST,
    user=settings.USER,
    password=settings.PASSWORD,
    database=settings.DATABASE,
    port=settings.PORT,
    pool_size=3
)

@contextmanager
def get_cursor(commit_on_exit=True):
    async with pool.connection() as connection:
        async with connection.cursor() as cursor:
            try:
                yield cursor
                if commit_on_exit:
                    await connection.commit()
            except Exception as e:
                await connection.rollback()
                raise e
            finally:
                await cursor.close()
                await connection.close()

async def get_members() -> List[MemberWithCategory]:
    async with get_cursor() as cursor:
        await cursor.execute("SELECT member.id, member.username, member.url_portfolio, member.date_validate, "
                             "member.date_deleted, GROUP_CONCAT(category.name) FROM member, member_has_category, category WHERE "
                             "member.id=member_has_category.id_member AND member_has_category.id_category=category.id GROUP BY "
                             "member.id")
        result = await cursor.fetchall()
        member_record = namedtuple("Member", ["id", "username", "url_portfolio", "date_validate", "date_deleted", "name"])
        return [map_member_record_to_member_with_category(member) for member in result
                if member.date_validate is not None and member.date_deleted is None]

def map_member_record_to_member_with_category(member_record: Any) -> MemberWithCategory:
    return MemberWithCategory(id_member=member_record.id, username=member_record.username, url_portfolio=member_record.url_portfolio,
                               category_name=member_record.name)

async def get_member_by_id(id_member: int) -> Optional[MemberIn]:
    async with get_cursor() as cursor:
        member_record = namedtuple("Member",
                                   ["id", "username", "firstname", "lastname", "description", "mail", "url_portfolio",
                                    "date_validate", "date_deleted"])
        query = "SELECT {} FROM member WHERE id = %(id)s".format(", ".join(member_record._fields))
        await cursor.execute(query, {'id': id_member})
        result = await cursor.fetchone()
        if result is None:
            return None
        member = member_record._make(result)
        return map_member_record_to_member_in(member)

def map_member_record_to_member_in(member_record: Any) -> MemberIn:
    return MemberIn(id=member_record.id, username=member_record.username, firstname=member_record.firstname, lastname=member_record.lastname,
                    description=member_record.description, mail=member_record.mail, url_portfolio=member_record.url_portfolio)

async def create_member(member: MemberIn) -> int:
    async with get_cursor() as cursor:
        sql = "INSERT INTO member (username, firstname, lastname, description, mail, url_portfolio) VALUES (%s, %s, %s, " \
              "%s, %s, %s)"
        val = (member.username, member.firstname, member.lastname, member.description, member.mail, member.url_portfolio)
        try:
            await cursor.execute(sql, val)
        except mysql.connector.Error:
            return "ErrorSQL: the request was unsuccessful..."
        id = cursor.lastrowid
        return id

async def patch_member_update(member: MemberOut) -> None:
    async with get_cursor() as cursor:
        sql = "UPDATE member SET firstname = %s, lastname = %s, description = %s, mail = %s, url_portfolio " \
              "= %s WHERE id = %s"
        val = (member.firstname, member.lastname, member.description, member.mail, member.url_portfolio, member.id)
        try:
            await cursor.execute(sql, val)
        except mysql.connector.Error:
            return "ErrorSQL: the request was unsuccessful..."
        return None
async def get_categories() -> List[Category]:
    async with get_cursor() as cursor:
        await cursor.execute("SELECT id, name FROM category")
        result = await cursor.fetchall()
        category_record = namedtuple("Category", ["id", "name"])
        return [map_category_record_to_category(category) for category in result]

def map_category_record_to_category(category_record: Any) -> Category:
    return Category(id=category_record.id, name=category_record.name)

async def post_category(category: CategoryOut) -> None:
    async with get_cursor() as cursor:
        sql = "INSERT INTO category (name) VALUES (%s)"
        val = [category.name]
        try:
            await cursor.execute(sql, val)
        except mysql.connector.Error as exc:
            return "ErrorSQL: the request was unsuccessful..."
        return None

async def get_members_category(name_category: str) -> List[GetMembers]:
    async with get_cursor() as cursor:
        sql = "SELECT member.* FROM member, member_has_category, category WHERE member.id = member_has_category.id_member " \
              "AND member_has_category.id_category = category.id AND category.name = %(name)s"
        await cursor.execute(sql, {"name": name_category})
        result = await cursor.fetchall()
        member_record = namedtuple("Member",
                                   ["id", "username", "lastname", "firstname", "description", "mail", "date_validate",
                                    "date_deleted", "url_portfolio"])
        return [map_member_record_to_get_members(member) for member in result
                if member.date_validate is not None and member.date_deleted is None]

def map_member_record_to_get_members(member_record: Any) -> GetMembers:
    return GetMembers(id=member_record.id, username=member_record.username, url_portfolio=member_record.url_portfolio)

async def return_id_category_by_name(name: str) -> int:
    async with get_cursor() as cursor:
        sql = "SELECT id FROM category WHERE name = %(name)s"
        try:
            await cursor.execute(sql, {"name": name})
            result = await cursor.fetchone()
            return result[0]
        except TypeError:
            return "ErrorSQL : the request was unsuccessful"

async def post_add_category_on_member(member: MemberHasCategory) -> None:
    async with get_cursor() as cursor:
        sql = """
        INSERT INTO member_has_category (id_member, id_category) VALUES (%s, %s)
        ON DUPLICATE KEY UPDATE id_member=id_member
        """
        try:
            values = []
            for cate in member.id_category:
                values.append([member.id_member, cate])
            await cursor.executemany(sql, values)
        except mysql.connector.Error:
            return "ErrorSQL: the request was unsuccessful..."
        return None
async def get_network_of_member_by_id(id_member: int) -> List[GetMemberHasNetwork]:
    async with get_cursor() as cursor:
        sql = "SELECT network.name, member_has_network.url, member_has_network.id_network FROM network, member_has_network, member WHERE member.id = " \
              "member_has_network.id_member AND member_has_network.id_network = network.id AND member.id = %(id)s"
        await cursor.execute(sql, {'id': id_member})
        result = await cursor.fetchall()
        network_record = namedtuple("Network", ["name", "url", "id_network"])
        return [map_network_record_to_get_member_has_network(network) for network in result]

def map_network_record_to_get_member_has_network(network_record: Any) -> GetMemberHasNetwork:
    return GetMemberHasNetwork(name=network_record.name, url=network_record.url, id_network=network_record.id_network)

async def get_category_of_member_by_id(id_member: int) -> List[CategoryOut]:
    async with get_cursor() as cursor:
        sql = "SELECT category.name FROM category, member, member_has_category WHERE member.id = " \
              "member_has_category.id_member AND member_has_category.id_category = category.id AND member.id = %(id)s"
        await cursor.execute(sql, {'id': id_member})
        result = await cursor.fetchall()
        category_record = namedtuple("Category", ["name"])
        return [map_category_record_to_category_out(category) for category in result]

def map_category_record_to_category_out(category_record: Any) -> CategoryOut:
    return CategoryOut(name=category_record.name)

async def get_member_has_category_by_id_member(id_member: int) -> List[MemberHasCategoryOut]:
    async with get_cursor() as cursor:
        sql = "SELECT member_has_category.id_member, category.name, member_has_category.id_category FROM " \
              "member, member_has_category, category WHERE member.id = member_has_category.id_member AND " \
              "member_has_category.id_category = category.id AND member.id = %(id)s"
        await cursor.execute(sql, {'id': id_member})
        result = await cursor.fetchall()
        category_record = namedtuple("MemberHasCategory", ["id_member","name","id_category"])
        return [map_category_record_to_member_has_category_out(category) for category in result]

def map_category_record_to_member_has_category_out(category_record: Any) -> MemberHasCategoryOut:
    return MemberHasCategoryOut(id_member=category_record.id_member, name=category_record.name, id_category=category_record.id_category)

async def get_network() -> List[Network]:
    async with get_cursor() as cursor:
        sql = "SELECT * FROM network"
        await cursor.execute(sql)
        result = await cursor.fetchall()
        network_record = namedtuple("Network", ["id", "name"])
        return [map_network_record_to_network(network) for network in result]

def map_network_record_to_network(network_record: Any) -> Network:
    return Network(id=network_record.id, name=network_record.name)

async def post_network_on_member(member: MemberHasNetwork) -> None:
    async with get_cursor() as cursor:
        sql = "INSERT INTO member_has_network (id_member, id_network, url) VALUES (%s, %s, %s) ON DUPLICATE KEY UPDATE " \
              "url = VALUES(url)"
        try:
            values = []
            for url, network in zip(member.url, member.id_network):
                if url != "" and url is not None:
                    values.append([member.id_member, network, url])
            await cursor.executemany(sql, values)
        except mysql.connector.Error as e:
            return "ErrorSQL: the request was unsuccessful..."
        return None

async def delete_category_delete_by_member(member: MemberHasCategory) -> None:
    async with get_cursor() as cursor:
        sql = "DELETE FROM member_has_category WHERE id_member = %s AND id_category = %s"
        try:
            values = []
            for cate in member.id_category:
                values.append([member.id_member, cate])
            await cursor.executemany(sql, values)
        except mysql.connector.Error:
            return "ErrorSQL: the request was unsuccessful..."
        return None

async def delete_network_delete_by_member(member: MemberHasNetworkIn) -> None:
    async with get_cursor() as cursor:
        sql = "DELETE FROM member_has_network WHERE id_member = %s AND id_network = %s"
        try:
            values = []
            for network in member.id_network:
                values.append([member.id_member, network])
            await cursor.executemany(sql, values)
        except mysql.connector.Error:
            return "ErrorSQL : the request was unsuccessful..."
        return None

async def add_new_network(name: NetworkOut) -> bool:
    async with get_cursor() as cursor:
        sql = "INSERT INTO network (name) VALUES (%s)"
        val = [name.name]
        try:
            await cursor.execute(sql, val)
        except mysql.connector.Error:
            await cursor.close()
            return "ErrorSQL: the request was unsuccessful..."
        return True

async def add_image_portfolio(file: UploadFile, id_member: int) -> None:
    async with get_cursor() as cursor:
        sql = "UPDATE member SET image_portfolio = %s WHERE id = %s"
        try:
            await cursor.execute(sql, (file.file.read(), id_member))
        except mysql.connector.Error:
            return "ErrorSQL : the request was unsuccessful..."
        await file.file.close()
        return None

async def get_image_by_id_member(id: int) -> bytes:
    async with get_cursor() as cursor:
        try:
            sql = "SELECT image_portfolio FROM member WHERE id = %(id)s"
            await cursor.execute(sql, {'id': id})
            result = await cursor.fetchone()
            image = result[0]
            return image
        except mysql.connector.Error as e:
            print(e)

async def register_new_member(name: str) -> int:
    async with get_cursor() as cursor:
        sql = "INSERT INTO member (username) VALUES (%s)"
        try:
            await cursor.execute(sql, (name,))
            id = cursor.lastrowid
        except mysql.connector.Error as exc:
            return "ErrorSQL: the request was unsuccessful..."
        return id

async def get_member_by_username(username: str) -> Optional[MemberIn]:
    async with get_cursor() as cursor:
        member_record = namedtuple("Member",
                                   ["id", "username", "firstname", "lastname", "description", "mail", "url_portfolio",
                                    "date_validate", "date_deleted"])
        query = "SELECT {} FROM member WHERE username = %(username)s".format(", ".join(member_record._fields))
        await cursor.execute(query, {'username': username})
        result = await cursor.fetchone()
        if result is None:
            return None
        member = member_record._make(result)
        return map_member_record_to_member_in(member)

async def register_token(access_token: str, refresh_token: str, id_user: int) -> None:
    async with get_cursor() as cursor:
        query = "INSERT INTO session (token_session, token_refresh, id_member) VALUES (%s, %s, %s)"
        val = (access_token, refresh_token, id_user)
        try:
            await cursor.execute(query, val)
        except mysql.connector.Error:
            return "Error SQL : the request was unsuccessfully..."
        return None

async def get_session(id_user: int) -> Optional[Session]:
    async with get_cursor() as cursor:
        session_record = namedtuple("Session",["access_token","refresh_token","id_member","date_created"])
        query = "SELECT * FROM session WHERE id_member = %(id_member)s"
        try:
            await cursor.execute(query, {'id_member': id_user})
            result = await cursor.fetchone()
            if not result:
                return None
            else:
                session = session_record._make(result)
                return session
        except mysql.connector.Error:
            return None

async def delete_session(id_user: int) -> None:
    async with get_cursor() as cursor:
        query = "DELETE FROM session WHERE id_member = %(id_member)s"
        try:
            await cursor.execute(query, {"id_member": id_user})
        except mysql.connector.Error:
            return "Error SQL"
        return None

async def verif_session(session: Session) -> Optional[bool]:
    async with get_cursor() as cursor:
        session_record = namedtuple("Session", ["access_token", "refresh_token", "id_member","date_created"])
        query = "SELECT * FROM session WHERE id_member = %(id_member)s"
        try:
            await cursor.execute(query, {'id_member': session["user_id"]})
            result = await cursor.fetchone()
            if not result:
                return None
            else:
                session_verif = session_record._make(result)
                print(session_verif)
                if session_verif.access_token == session["access_token"] and session_verif.refresh_token == session["refresh_token"]:
                    temps_date = timedelta(minutes=60)
                    if session_verif.date_created + temps_date > datetime.now():
                        return True
                    else:
                        return None
                else:
                    return None
        except mysql.connector.Error:
            return None

async def is_admin(id_user: int) -> bool:
    async with get_cursor() as cursor:
        query = "SELECT is_admin FROM member WHERE id = %(id)s"
        try:
            await cursor.execute(query, {"id": id_user})
            result = await cursor.fetchone()
            if result and result[0] == 1:
                return True
            else:
                return False
        except mysql.connector.Error:
            return False

async def delete_table_member_has_category(name: str) -> None:
    async with get_cursor() as cursor:
        sql = "DELETE FROM member_has_category WHERE id_category = (" \
              "SELECT id FROM category WHERE name = %(name)s)"
        try:
            await cursor.execute(sql, {"name": name})
        except mysql.connector.Error:
            return "ErrorSQL : ..."
        return None

async def delete_category(name: str) -> None:
    async with get_cursor() as cursor:
        await delete_table_member_has_category(name)
        sql = "DELETE FROM category WHERE name = %(name)s"
        try:
            await cursor.execute(sql, {"name": name})
        except mysql.connector.Error:
            return "ErrorSQL : the request was unsuccessful..."
        return None

async def delete_table_member_has_network(name: str) -> None:
    async with get_cursor() as cursor:
        sql = "DELETE FROM member_has_network WHERE id_network = (" \
              "SELECT id FROM network WHERE name = %(name)s)"
        try:
            await cursor.execute(sql, {"name": name})
        except mysql.connector.Error:
            return "ErrorSQL : ..."
        return None

async def delete_network(name: str) -> None:
    async with get_cursor() as cursor:
        await delete_table_member_has_network(name)
        sql = "DELETE FROM network WHERE name = %(name)s"
        try:
            await cursor.execute(sql, {"name": name})
        except mysql.connector.Error:
            return "ErrorSQL : the request was unsuccessful..."
        return None

async def get_all_member_admin() -> List[MemberOut]:
    async with get_cursor() as cursor:
        await cursor.execute("SELECT id, username, firstname, lastname, description, mail, url_portfolio, date_validate, "
                             "date_deleted FROM member")
        column_names = [column[0] for column in cursor.description]
        MemberTuple = namedtuple("Member", column_names)
        result = await cursor.fetchall()
        return [map_member_tuple_to_member_out(member) for member in result]

def map_member_tuple_to_member_out(member: Any) -> MemberOut:
    return MemberOut(id=member.id, username=member.username, firstname=member.firstname, lastname=member.lastname,
                      description=member.description, mail=member.mail, url_portfolio=member.url_portfolio,
                      date_activated=member.date_validate, date_deleted=member.date_deleted)

async def validate_member(id_member: int) -> None:
    async with get_cursor() as cursor:
        sql = "UPDATE member SET date_validate = NOW() WHERE id = %(id)s"
        try:
            await cursor.execute(sql, {"id": id_member})
        except mysql.connector.Error:
            return "ErrorSQL: the request was unsuccessful..."
        return None

async def ban_member(id_member: int) -> None:
    async with get_cursor() as cursor:
        sql = "UPDATE member SET date_deleted = NOW() WHERE id = %(id)s"
        try:
            await cursor.execute(sql, {"id": id_member})
        except mysql.connector.Error:
            return "ErrorSQL: the request was unsuccessful..."
        return None

async def unban_member(id_member: int) -> None:
    async with get_cursor() as cursor:
        sql = "UPDATE member SET date_deleted = null WHERE id = %(id)s"
        try:
            await cursor.execute(sql, {"id": id_member})
        except mysql.connector.Error:
            return "ErrorSQL: the request was unsuccessful..."
        return None
